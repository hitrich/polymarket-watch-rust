use crate::engine::{
    system_now_ms, OperatorCommand, OperatorCommandRequest, OperatorCommandSender,
};
use crate::error::{BotError, Result};
use crate::runtime::StartupReport;
use crate::state::SharedRuntimeState;
use rand::{rngs::SysRng, TryRng as _};
use serde_json::json;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const MAX_GUI_CONNECTIONS: usize = 32;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
pub struct GuiState {
    control_token: String,
}

impl GuiState {
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            control_token: new_control_token()?,
        })
    }
}

pub fn run_gui(
    bind_addr: &str,
    report: StartupReport,
    runtime: SharedRuntimeState,
    commands: OperatorCommandSender,
    shutdown: CancellationToken,
) -> Result<()> {
    validate_gui_bind_addr(bind_addr)?;
    let listener = TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    println!("polymarket-rs control plane listening on http://{bind_addr}");

    let report = Arc::new(report);
    let gui = Arc::new(Mutex::new(GuiState::try_new()?));
    let active_connections = Arc::new(AtomicUsize::new(0));
    while !shutdown.is_cancelled() {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let Some(permit) = ConnectionPermit::try_acquire(Arc::clone(&active_connections))
                else {
                    write_response(
                        &mut stream,
                        &http_response(
                            503,
                            "Service Unavailable",
                            "application/json",
                            r#"{"error":"dashboard_connection_limit_reached"}"#,
                        ),
                    )?;
                    continue;
                };
                let report = Arc::clone(&report);
                let runtime = Arc::clone(&runtime);
                let gui = Arc::clone(&gui);
                let commands = commands.clone();
                let connection_shutdown = shutdown.clone();
                let bind_addr = bind_addr.to_string();
                std::thread::spawn(move || {
                    let _permit = permit;
                    if let Err(error) = handle_connection(
                        stream,
                        &bind_addr,
                        &report,
                        &runtime,
                        &gui,
                        &commands,
                        &connection_shutdown,
                    ) {
                        if !connection_shutdown.is_cancelled() && !benign_client_disconnect(&error)
                        {
                            eprintln!("gui request failed: {error}");
                        }
                    }
                });
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn benign_client_disconnect(error: &BotError) -> bool {
    matches!(
        error,
        BotError::Io(detail)
            if detail.contains("Resource temporarily unavailable")
                || detail.contains("timed out")
                || detail.contains("connection reset")
                || detail.contains("Broken pipe")
    )
}

struct ConnectionPermit {
    active_connections: Arc<AtomicUsize>,
}

impl ConnectionPermit {
    fn try_acquire(active_connections: Arc<AtomicUsize>) -> Option<Self> {
        let prior = active_connections.fetch_add(1, Ordering::AcqRel);
        if prior >= MAX_GUI_CONNECTIONS {
            active_connections.fetch_sub(1, Ordering::AcqRel);
            None
        } else {
            Some(Self { active_connections })
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn validate_gui_bind_addr(bind_addr: &str) -> Result<()> {
    if bind_addr.starts_with("localhost:") {
        return Ok(());
    }
    match bind_addr.parse::<SocketAddr>() {
        Ok(address) if address.ip().is_loopback() => Ok(()),
        _ => Err(BotError::Config(
            "gui_bind_must_be_loopback_use_127.0.0.1_or_localhost".to_string(),
        )),
    }
}

fn handle_connection(
    mut stream: TcpStream,
    bind_addr: &str,
    report: &StartupReport,
    runtime: &SharedRuntimeState,
    gui: &Arc<Mutex<GuiState>>,
    commands: &OperatorCommandSender,
    shutdown: &CancellationToken,
) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let request = read_http_request(&mut stream)?;
    let (header_block, body) = request
        .split_once("\r\n\r\n")
        .ok_or_else(|| BotError::Parse("incomplete_http_headers".to_string()))?;
    let request_line = header_block.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let raw_path = parts.next().unwrap_or("/");
    let version = parts.next().unwrap_or_default();
    if parts.next().is_some() || version != "HTTP/1.1" {
        return write_response(
            &mut stream,
            &http_response(
                400,
                "Bad Request",
                "application/json",
                r#"{"error":"invalid_request_line"}"#,
            ),
        );
    }
    let path = raw_path.split('?').next().unwrap_or(raw_path);
    if !host_allowed(header_block, bind_addr) {
        return write_response(
            &mut stream,
            &http_response(
                421,
                "Misdirected Request",
                "application/json",
                r#"{"error":"gui_host_not_allowed"}"#,
            ),
        );
    }

    match (method, path) {
        ("GET", "/") => {
            let token = gui
                .lock()
                .map_err(|_| BotError::Execution("gui_state_lock_poisoned".to_string()))?
                .control_token
                .clone();
            let html =
                include_str!("../assets/dashboard.html").replace("__COMMAND_TOKEN__", &token);
            write_response(&mut stream, &http_ok("text/html; charset=utf-8", &html))
        }
        ("GET", "/assets/dashboard.css") => write_response(
            &mut stream,
            &http_ok(
                "text/css; charset=utf-8",
                include_str!("../assets/dashboard.css"),
            ),
        ),
        ("GET", "/assets/dashboard.js") => write_response(
            &mut stream,
            &http_ok(
                "text/javascript; charset=utf-8",
                include_str!("../assets/dashboard.js"),
            ),
        ),
        ("GET", "/assets/tabler-icons.css") => write_response(
            &mut stream,
            &http_ok(
                "text/css; charset=utf-8",
                include_str!("../assets/tabler-icons.css"),
            ),
        ),
        ("GET", "/assets/fonts/tabler-icons.woff2") => write_embedded_asset(
            &mut stream,
            "font/woff2",
            include_bytes!("../assets/fonts/tabler-icons.woff2"),
        ),
        ("GET", "/assets/world-network-map.jpg") => write_embedded_asset(
            &mut stream,
            "image/jpeg",
            include_bytes!("../assets/world-network-map.jpg"),
        ),
        ("GET", "/favicon.ico") => write_response(
            &mut stream,
            &http_response(204, "No Content", "image/x-icon", ""),
        ),
        ("GET", "/api/status") => {
            let supplied_token = header_value(header_block, "x-control-token");
            let expected_token = gui
                .lock()
                .map_err(|_| BotError::Execution("gui_state_lock_poisoned".to_string()))?
                .control_token
                .clone();
            if !supplied_token
                .is_some_and(|token| constant_time_eq(token.as_bytes(), expected_token.as_bytes()))
            {
                return write_response(
                    &mut stream,
                    &http_response(
                        403,
                        "Forbidden",
                        "application/json",
                        r#"{"error":"gui_status_token_invalid"}"#,
                    ),
                );
            }
            let body = render_status_json(report, runtime)?;
            write_response(&mut stream, &http_ok("application/json", &body))
        }
        ("POST", "/api/command") => {
            if !post_origin_allowed(header_block, bind_addr) {
                return write_response(
                    &mut stream,
                    &http_response(
                        403,
                        "Forbidden",
                        "application/json",
                        &error_json(&BotError::Compliance("gui_origin_not_allowed".to_string())),
                    ),
                );
            }
            if !header_value(header_block, "content-type")
                .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"))
            {
                return write_response(
                    &mut stream,
                    &http_response(
                        415,
                        "Unsupported Media Type",
                        "application/json",
                        r#"{"error":"form_content_type_required"}"#,
                    ),
                );
            }
            let action = parse_form_value(body, "action")?;
            let token = parse_form_value(body, "token")?;
            let expected_token = gui
                .lock()
                .map_err(|_| BotError::Execution("gui_state_lock_poisoned".to_string()))?
                .control_token
                .clone();
            if !constant_time_eq(token.as_bytes(), expected_token.as_bytes()) {
                return write_response(
                    &mut stream,
                    &http_response(
                        403,
                        "Forbidden",
                        "application/json",
                        &error_json(&BotError::Compliance(
                            "gui_command_token_invalid".to_string(),
                        )),
                    ),
                );
            }
            let command = match parse_operator_command(&action, report) {
                Ok(command) => command,
                Err(error) => {
                    return write_response(
                        &mut stream,
                        &http_response(423, "Locked", "application/json", &error_json(&error)),
                    );
                }
            };
            let (response_tx, response_rx) = std::sync::mpsc::channel();
            commands
                .send(OperatorCommandRequest {
                    command,
                    response: response_tx,
                })
                .map_err(|_| BotError::Execution("runtime_command_channel_closed".to_string()))?;
            let outcome = response_rx
                .recv_timeout(COMMAND_TIMEOUT)
                .map_err(|_| BotError::Execution("runtime_command_timeout".to_string()))?;
            match outcome {
                Ok(value) => {
                    let body = serde_json::to_string(&value).map_err(|error| {
                        BotError::Execution(format!("command_response_serialize:{error}"))
                    })?;
                    let response = write_response(&mut stream, &http_ok("application/json", &body));
                    if action == "shutdown" {
                        // Do not terminate the process until the client has had a chance to
                        // receive the accepted response. A failed write still shuts down: the
                        // operator command was already accepted by the runtime.
                        shutdown.cancel();
                    }
                    response
                }
                Err(error) => write_response(
                    &mut stream,
                    &http_response(423, "Locked", "application/json", &error_json(&error)),
                ),
            }
        }
        _ => write_response(
            &mut stream,
            &http_response(
                404,
                "Not Found",
                "application/json",
                r#"{"error":"not_found"}"#,
            ),
        ),
    }
}

fn render_status_json(report: &StartupReport, runtime: &SharedRuntimeState) -> Result<String> {
    let runtime = runtime
        .read()
        .map_err(|_| BotError::Execution("runtime_state_lock_poisoned".to_string()))?
        .clone();
    let readiness = report
        .readiness_gates()
        .into_iter()
        .map(|gate| {
            let passed = runtime
                .readiness
                .get(gate.name)
                .copied()
                .unwrap_or(gate.passed);
            json!({
                "name": gate.name,
                "passed": passed,
                "blocking": !passed,
                "detail": gate.detail,
            })
        })
        .collect::<Vec<_>>();
    let live_lock_reasons = if runtime.live_submission_enabled {
        Vec::new()
    } else {
        report.live_lock_reasons.clone()
    };
    serde_json::to_string(&json!({
        "server_time_ms": system_now_ms(),
        "startup": {
            "mode": report.mode_label(),
            "version": env!("CARGO_PKG_VERSION"),
            "live_status": if runtime.live_submission_enabled { "LIVE ENABLED" } else { "LIVE LOCKED" },
            "live_submission_enabled": runtime.live_submission_enabled,
            "readiness": readiness,
            "missing_secrets": report.secrets.missing_names(),
            "live_lock_reasons": live_lock_reasons,
            "warnings": report.warnings,
        },
        "runtime": runtime,
    }))
    .map_err(|error| BotError::Execution(format!("status_serialize:{error}")))
}

fn parse_operator_command(action: &str, report: &StartupReport) -> Result<OperatorCommand> {
    match action {
        "pause" => Ok(OperatorCommand::Pause),
        "resume_paper" => Ok(OperatorCommand::ResumePaper),
        "cancel_stale" => Ok(OperatorCommand::CancelStale),
        "cancel_all" => Ok(OperatorCommand::CancelAll),
        "flatten_paper" => Ok(OperatorCommand::FlattenPaper),
        "reduce_risk" => Ok(OperatorCommand::ReduceRisk),
        "shutdown" => Ok(OperatorCommand::Shutdown),
        "resume_live" => Ok(OperatorCommand::ResumeLive),
        "enable_live" => Err(BotError::Readiness(format!(
            "live_unlock_rejected:{}",
            report.live_lock_reasons.join(",")
        ))),
        _ => Err(BotError::Execution(format!("unknown_command:{action}"))),
    }
}

fn read_http_request(stream: &mut impl Read) -> Result<String> {
    let mut request = Vec::with_capacity(4 * 1024);
    let mut chunk = [0u8; 4 * 1024];
    let mut required_len = None;
    loop {
        if request.len() > MAX_HTTP_HEADER_BYTES + MAX_HTTP_BODY_BYTES {
            return Err(BotError::Parse("http_request_too_large".to_string()));
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if required_len.is_none() {
            if let Some(header_end) = find_bytes(&request, b"\r\n\r\n") {
                if header_end > MAX_HTTP_HEADER_BYTES {
                    return Err(BotError::Parse("http_headers_too_large".to_string()));
                }
                let headers = String::from_utf8(request[..header_end].to_vec())
                    .map_err(|_| BotError::Parse("http_headers_not_utf8".to_string()))?;
                if !header_values(&headers, "transfer-encoding").is_empty() {
                    return Err(BotError::Parse(
                        "http_transfer_encoding_not_supported".to_string(),
                    ));
                }
                let lengths = header_values(&headers, "content-length");
                if lengths.len() > 1 {
                    return Err(BotError::Parse("duplicate_http_content_length".to_string()));
                }
                let content_length = lengths
                    .first()
                    .map(|value| {
                        value
                            .parse::<usize>()
                            .map_err(|_| BotError::Parse("invalid_http_content_length".to_string()))
                    })
                    .transpose()?
                    .unwrap_or(0);
                if content_length > MAX_HTTP_BODY_BYTES {
                    return Err(BotError::Parse("http_body_too_large".to_string()));
                }
                required_len = Some(header_end + 4 + content_length);
            } else if request.len() > MAX_HTTP_HEADER_BYTES {
                return Err(BotError::Parse("http_headers_too_large".to_string()));
            }
        }
        if required_len.is_some_and(|required| request.len() >= required) {
            break;
        }
    }
    let required_len =
        required_len.ok_or_else(|| BotError::Parse("incomplete_http_headers".to_string()))?;
    if request.len() < required_len {
        return Err(BotError::Parse("incomplete_http_body".to_string()));
    }
    request.truncate(required_len);
    String::from_utf8(request).map_err(|_| BotError::Parse("http_request_not_utf8".to_string()))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn header_values(headers: &str, name: &str) -> Vec<String> {
    headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.trim()
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
        .collect()
}

fn header_value(headers: &str, name: &str) -> Option<String> {
    let values = header_values(headers, name);
    (values.len() == 1).then(|| values[0].clone())
}

fn host_allowed(headers: &str, bind_addr: &str) -> bool {
    let Some(host) = header_value(headers, "host") else {
        return false;
    };
    allowed_origins(bind_addr).iter().any(|origin| {
        origin
            .strip_prefix("http://")
            .is_some_and(|allowed| allowed.eq_ignore_ascii_case(&host))
    })
}

fn parse_form_value(body: &str, field: &str) -> Result<String> {
    for part in body.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        if key == field {
            return percent_decode(value);
        }
    }
    Ok(String::new())
}

fn percent_decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'%' => return Err(BotError::Parse("invalid_percent_encoding".to_string())),
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| BotError::Parse("form_value_not_utf8".to_string()))
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(BotError::Parse("invalid_percent_encoding".to_string())),
    }
}

fn post_origin_allowed(headers: &str, bind_addr: &str) -> bool {
    if let Some(origin) = header_value(headers, "origin") {
        return allowed_origins(bind_addr)
            .iter()
            .any(|value| value == &origin);
    }
    if let Some(referer) = header_value(headers, "referer") {
        return allowed_origins(bind_addr)
            .iter()
            .any(|value| referer == *value || referer.starts_with(&format!("{value}/")));
    }
    false
}

fn allowed_origins(bind_addr: &str) -> Vec<String> {
    let mut origins = vec![format!("http://{bind_addr}")];
    if let Some(port) = bind_addr.strip_prefix("127.0.0.1:") {
        origins.push(format!("http://localhost:{port}"));
    }
    if let Some(port) = bind_addr.strip_prefix("localhost:") {
        origins.push(format!("http://127.0.0.1:{port}"));
    }
    origins
}

fn write_response(stream: &mut TcpStream, response: &str) -> Result<()> {
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn write_embedded_asset(stream: &mut TcpStream, content_type: &str, body: &[u8]) -> Result<()> {
    // These embedded assets exceed common socket send buffers. A longer loopback-only
    // write window prevents otherwise healthy font and image responses from truncating.
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write_binary_response(stream, content_type, body)
}

fn write_binary_response(stream: &mut impl Write, content_type: &str, body: &[u8]) -> Result<()> {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nCross-Origin-Resource-Policy: same-origin\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn http_ok(content_type: &str, body: &str) -> String {
    http_response(200, "OK", content_type, body)
}

fn http_response(status: u16, reason: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\nCross-Origin-Resource-Policy: same-origin\r\nContent-Security-Policy: default-src 'self'; style-src 'self'; script-src 'self'; font-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'\r\nPermissions-Policy: camera=(), microphone=(), geolocation=(), payment=()\r\n\r\n{body}",
        body.len()
    )
}

fn error_json(error: &BotError) -> String {
    json!({"status":"rejected", "error":error.to_string()}).to_string()
}

fn new_control_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    SysRng
        .try_fill_bytes(&mut bytes)
        .map_err(|error| BotError::Io(format!("control_token_entropy:{error}")))?;
    Ok(hex_encode(&bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq as _;
    left.ct_eq(right).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_bind_is_allowed() {
        assert!(validate_gui_bind_addr("127.0.0.1:8787").is_ok());
        assert!(validate_gui_bind_addr("localhost:8787").is_ok());
        assert!(validate_gui_bind_addr("0.0.0.0:8787").is_err());
    }

    #[test]
    fn request_reader_rejects_duplicate_content_length() {
        let raw = b"POST /api/command HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n";
        let error = read_http_request(&mut &raw[..]).unwrap_err();
        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn host_validation_blocks_dns_rebinding_and_duplicate_headers() {
        assert!(host_allowed(
            "GET / HTTP/1.1\r\nHost: 127.0.0.1:8787",
            "127.0.0.1:8787"
        ));
        assert!(host_allowed(
            "GET / HTTP/1.1\r\nHost: localhost:8787",
            "127.0.0.1:8787"
        ));
        assert!(!host_allowed(
            "GET / HTTP/1.1\r\nHost: attacker.example:8787",
            "127.0.0.1:8787"
        ));
        assert!(!host_allowed("GET / HTTP/1.1", "127.0.0.1:8787"));
        assert!(!host_allowed(
            "GET / HTTP/1.1\r\nHost: 127.0.0.1:8787\r\nHost: attacker.example:8787",
            "127.0.0.1:8787"
        ));
    }

    #[test]
    fn percent_decoder_is_strict() {
        assert_eq!(percent_decode("cancel%5Fall").unwrap(), "cancel_all");
        assert!(percent_decode("bad%xx").is_err());
        assert!(percent_decode("bad%").is_err());
    }

    #[test]
    fn control_tokens_are_random_and_constant_time_comparable() {
        let first = new_control_token().unwrap();
        let second = new_control_token().unwrap();
        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
        assert!(constant_time_eq(first.as_bytes(), first.as_bytes()));
        assert!(!constant_time_eq(first.as_bytes(), second.as_bytes()));
    }

    #[test]
    fn binary_asset_response_has_exact_length_and_body() {
        let body = [0_u8, 1, 2, 255];
        let mut response = Vec::new();
        write_binary_response(&mut response, "application/octet-stream", &body).unwrap();
        let separator = b"\r\n\r\n";
        let split = response
            .windows(separator.len())
            .position(|window| window == separator)
            .unwrap();
        let headers = std::str::from_utf8(&response[..split]).unwrap();
        assert!(headers.contains("HTTP/1.1 200 OK"));
        assert!(headers.contains("Content-Length: 4"));
        assert!(headers.contains("X-Content-Type-Options: nosniff"));
        assert_eq!(&response[split + separator.len()..], &body);
    }
}
