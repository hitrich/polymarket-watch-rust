use crate::error::{BotError, Result};
use crate::fixed::Fixed;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BehavioralZone {
    Neutral,
    NearPanicThreshold,
    ThroughPanicThreshold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedFlow {
    Normal,
    ElevatedRetailSelling,
    PanicSelling,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehavioralPressureConfig {
    pub panic_threshold_bps: i64,
    pub pre_threshold_buffer_bps: i64,
    pub expected_cascade_bps: i64,
}

impl Default for BehavioralPressureConfig {
    fn default() -> Self {
        Self {
            panic_threshold_bps: -1500,
            pre_threshold_buffer_bps: 20,
            expected_cascade_bps: 150,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehavioralPressure {
    pub drawdown_bps: i64,
    pub zone: BehavioralZone,
    pub expected_flow: ExpectedFlow,
    pub expected_adverse_move_bps: i64,
}

pub fn estimate_behavioral_pressure(
    reference_price: Fixed,
    current_price: Fixed,
    config: &BehavioralPressureConfig,
) -> Result<BehavioralPressure> {
    if reference_price.raw() <= 0 {
        return Err(BotError::Risk(
            "behavior_reference_price_must_be_positive".to_string(),
        ));
    }
    if config.panic_threshold_bps >= 0 {
        return Err(BotError::Risk(
            "panic_threshold_bps_must_be_negative".to_string(),
        ));
    }
    if config.pre_threshold_buffer_bps < 0 {
        return Err(BotError::Risk(
            "pre_threshold_buffer_bps_must_be_non_negative".to_string(),
        ));
    }
    if config.expected_cascade_bps < 0 {
        return Err(BotError::Risk(
            "expected_cascade_bps_must_be_non_negative".to_string(),
        ));
    }

    let price_delta = current_price.checked_sub(reference_price)?.raw();
    let drawdown_bps_i128 = price_delta
        .checked_mul(10_000)
        .and_then(|scaled| scaled.checked_div(reference_price.raw()))
        .ok_or_else(|| BotError::Risk("behavior_drawdown_calculation_overflow".to_string()))?;
    let drawdown_bps = i64::try_from(drawdown_bps_i128)
        .map_err(|_| BotError::Risk("behavior_drawdown_bps_overflow".to_string()))?;
    let near_threshold_bps = config
        .panic_threshold_bps
        .checked_add(config.pre_threshold_buffer_bps)
        .ok_or_else(|| BotError::Risk("behavior_threshold_overflow".to_string()))?;

    let (zone, expected_flow, expected_adverse_move_bps) =
        if drawdown_bps <= config.panic_threshold_bps {
            (
                BehavioralZone::ThroughPanicThreshold,
                ExpectedFlow::PanicSelling,
                config.expected_cascade_bps,
            )
        } else if drawdown_bps <= near_threshold_bps {
            (
                BehavioralZone::NearPanicThreshold,
                ExpectedFlow::ElevatedRetailSelling,
                config
                    .panic_threshold_bps
                    .checked_sub(drawdown_bps)
                    .ok_or_else(|| BotError::Risk("behavior_adverse_move_overflow".to_string()))?,
            )
        } else {
            (BehavioralZone::Neutral, ExpectedFlow::Normal, 0)
        };

    Ok(BehavioralPressure {
        drawdown_bps,
        zone,
        expected_flow,
        expected_adverse_move_bps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_minus_1480_as_near_panic_threshold() {
        let pressure = estimate_behavioral_pressure(
            "1".parse().unwrap(),
            "0.852".parse().unwrap(),
            &BehavioralPressureConfig::default(),
        )
        .unwrap();
        assert_eq!(pressure.drawdown_bps, -1480);
        assert_eq!(pressure.zone, BehavioralZone::NearPanicThreshold);
        assert_eq!(pressure.expected_flow, ExpectedFlow::ElevatedRetailSelling);
    }

    #[test]
    fn detects_minus_15_percent_as_through_threshold() {
        let pressure = estimate_behavioral_pressure(
            "1".parse().unwrap(),
            "0.85".parse().unwrap(),
            &BehavioralPressureConfig::default(),
        )
        .unwrap();
        assert_eq!(pressure.drawdown_bps, -1500);
        assert_eq!(pressure.zone, BehavioralZone::ThroughPanicThreshold);
        assert_eq!(pressure.expected_flow, ExpectedFlow::PanicSelling);
    }

    #[test]
    fn rejects_extreme_arithmetic_and_negative_cascade_config() {
        let extreme = estimate_behavioral_pressure(
            Fixed::from_scaled(1),
            Fixed::from_scaled(i128::MAX),
            &BehavioralPressureConfig::default(),
        )
        .unwrap_err();
        assert!(extreme.to_string().contains("overflow"));

        let config = BehavioralPressureConfig {
            expected_cascade_bps: -1,
            ..BehavioralPressureConfig::default()
        };
        let err = estimate_behavioral_pressure(Fixed::ONE, Fixed::ONE, &config).unwrap_err();
        assert!(err.to_string().contains("expected_cascade"));
    }
}
