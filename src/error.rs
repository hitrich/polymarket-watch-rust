use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, BotError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotError {
    Config(String),
    Compliance(String),
    Readiness(String),
    Risk(String),
    Journal(String),
    Execution(String),
    Protocol(String),
    Parse(String),
    Io(String),
}

impl Display for BotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(v) => write!(f, "config error: {v}"),
            Self::Compliance(v) => write!(f, "compliance error: {v}"),
            Self::Readiness(v) => write!(f, "readiness error: {v}"),
            Self::Risk(v) => write!(f, "risk error: {v}"),
            Self::Journal(v) => write!(f, "journal error: {v}"),
            Self::Execution(v) => write!(f, "execution error: {v}"),
            Self::Protocol(v) => write!(f, "protocol error: {v}"),
            Self::Parse(v) => write!(f, "parse error: {v}"),
            Self::Io(v) => write!(f, "io error: {v}"),
        }
    }
}

impl std::error::Error for BotError {}

impl From<std::io::Error> for BotError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
