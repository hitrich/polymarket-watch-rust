use crate::error::{BotError, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const SCALE: i128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Fixed(i128);

impl Fixed {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(SCALE);

    pub const fn from_scaled(raw: i128) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> i128 {
        self.0
    }

    pub fn checked_mul(self, rhs: Self) -> Result<Self> {
        self.0
            .checked_mul(rhs.0)
            .and_then(|v| v.checked_div(SCALE))
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point multiplication overflow".to_string()))
    }

    /// Multiplies and rounds toward positive infinity at the fixed-point scale.
    /// Risk limits use this variant so a sub-micro-unit remainder is never
    /// discarded from positive notional or exposure.
    pub fn checked_mul_ceil(self, rhs: Self) -> Result<Self> {
        let product = self
            .0
            .checked_mul(rhs.0)
            .ok_or_else(|| BotError::Risk("fixed-point multiplication overflow".to_string()))?;
        let quotient = product.div_euclid(SCALE);
        let rounded = if product.rem_euclid(SCALE) == 0 {
            quotient
        } else {
            quotient
                .checked_add(1)
                .ok_or_else(|| BotError::Risk("fixed-point multiplication overflow".to_string()))?
        };
        Ok(Self(rounded))
    }

    pub fn checked_div(self, rhs: Self) -> Result<Self> {
        if rhs.0 == 0 {
            return Err(BotError::Risk("fixed-point division by zero".to_string()));
        }
        self.0
            .checked_mul(SCALE)
            .and_then(|value| value.checked_div(rhs.0))
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point division overflow".to_string()))
    }

    pub fn checked_div_int(self, rhs: i128) -> Result<Self> {
        if rhs == 0 {
            return Err(BotError::Risk("fixed-point division by zero".to_string()));
        }
        self.0
            .checked_div(rhs)
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point division overflow".to_string()))
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point addition overflow".to_string()))
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point subtraction overflow".to_string()))
    }

    pub fn checked_abs(self) -> Result<Self> {
        self.0
            .checked_abs()
            .map(Self)
            .ok_or_else(|| BotError::Risk("fixed-point absolute overflow".to_string()))
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn ticks_between(self, other: Self, tick: Self) -> Result<u64> {
        if tick.0 <= 0 {
            return Err(BotError::Risk("tick size must be positive".to_string()));
        }
        let difference = self.checked_sub(other)?;
        let absolute = difference.checked_abs()?.0;
        let ticks = absolute / tick.0;
        u64::try_from(ticks).map_err(|_| BotError::Risk("tick distance exceeds u64".to_string()))
    }

    pub fn is_aligned_to_tick(self, tick: Self) -> bool {
        tick.0 > 0 && self.0 % tick.0 == 0
    }

    pub fn floor_to_decimals(self, decimals: u32) -> Result<Self> {
        if decimals > 6 {
            return Err(BotError::Risk(
                "fixed-point decimal precision exceeds six".to_string(),
            ));
        }
        let factor = 10i128
            .checked_pow(6 - decimals)
            .ok_or_else(|| BotError::Risk("fixed-point quantization overflow".to_string()))?;
        Ok(Self(self.0.div_euclid(factor) * factor))
    }
}

impl FromStr for Fixed {
    type Err = BotError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Err(BotError::Parse("empty fixed-point value".to_string()));
        }

        let negative = s.starts_with('-');
        let value = if negative { &s[1..] } else { s };
        let mut parts = value.split('.');
        let whole = parts
            .next()
            .ok_or_else(|| BotError::Parse(format!("invalid fixed-point value: {s}")))?;
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some() {
            return Err(BotError::Parse(format!("invalid fixed-point value: {s}")));
        }
        if fraction.len() > 6 {
            return Err(BotError::Parse(format!(
                "too many decimal places for fixed-point value: {s}"
            )));
        }
        if whole.is_empty() && fraction.is_empty() {
            return Err(BotError::Parse(format!("invalid fixed-point value: {s}")));
        }

        let whole = if whole.is_empty() {
            0
        } else {
            whole
                .parse::<i128>()
                .map_err(|_| BotError::Parse(format!("invalid fixed-point value: {s}")))?
        };
        let mut frac = 0i128;
        let mut scale = 100_000i128;
        for ch in fraction.chars() {
            let digit = ch
                .to_digit(10)
                .ok_or_else(|| BotError::Parse(format!("invalid fixed-point value: {s}")))?;
            frac += i128::from(digit) * scale;
            scale /= 10;
        }

        let raw = whole
            .checked_mul(SCALE)
            .and_then(|v| v.checked_add(frac))
            .ok_or_else(|| BotError::Parse(format!("fixed-point overflow: {s}")))?;
        Ok(Self(if negative { -raw } else { raw }))
    }
}

impl Serialize for Fixed {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct FixedVisitor;

impl Visitor<'_> for FixedVisitor {
    type Value = Fixed;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an exact fixed-point decimal string")
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }
}

impl<'de> Deserialize<'de> for Fixed {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(FixedVisitor)
    }
}

impl Display for Fixed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let value = self.0.unsigned_abs();
        let whole = value / SCALE as u128;
        let fraction = value % SCALE as u128;
        if fraction == 0 {
            return write!(f, "{sign}{whole}");
        }
        let mut frac = format!("{fraction:06}");
        while frac.ends_with('0') {
            frac.pop();
        }
        write!(f, "{sign}{whole}.{frac}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats_fixed_values() {
        assert_eq!("0.123456".parse::<Fixed>().unwrap().to_string(), "0.123456");
        assert_eq!("10.5".parse::<Fixed>().unwrap().to_string(), "10.5");
        assert_eq!(".48".parse::<Fixed>().unwrap().to_string(), "0.48");
        assert!(".".parse::<Fixed>().is_err());
        assert!("0.1234567".parse::<Fixed>().is_err());
    }

    #[test]
    fn serde_round_trips_as_an_exact_string() {
        let value = ".48".parse::<Fixed>().unwrap();
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(encoded, "\"0.48\"");
        assert_eq!(serde_json::from_str::<Fixed>(&encoded).unwrap(), value);
    }

    #[test]
    fn computes_notional_without_float() {
        let price = "0.42".parse::<Fixed>().unwrap();
        let size = "10".parse::<Fixed>().unwrap();
        assert_eq!(price.checked_mul(size).unwrap().to_string(), "4.2");
    }

    #[test]
    fn conservative_multiplication_rounds_positive_remainder_up() {
        let one_micro = Fixed::from_scaled(1);
        assert_eq!(one_micro.checked_mul(one_micro).unwrap(), Fixed::ZERO);
        assert_eq!(one_micro.checked_mul_ceil(one_micro).unwrap(), one_micro);
        assert_eq!(
            "0.42"
                .parse::<Fixed>()
                .unwrap()
                .checked_mul_ceil("10".parse::<Fixed>().unwrap())
                .unwrap()
                .to_string(),
            "4.2"
        );
    }

    #[test]
    fn checked_add_rejects_overflow() {
        let max = Fixed::from_scaled(i128::MAX);
        assert!(max.checked_add(Fixed::ONE).is_err());
    }

    #[test]
    fn tick_distance_rejects_subtraction_and_conversion_overflow() {
        assert!(Fixed::from_scaled(i128::MAX)
            .ticks_between(Fixed::from_scaled(i128::MIN), Fixed::ONE)
            .is_err());
        assert!(Fixed::from_scaled(i128::from(u64::MAX) + 1)
            .ticks_between(Fixed::ZERO, Fixed::from_scaled(1))
            .is_err());
        let minimum = Fixed::from_scaled(i128::MIN);
        assert!(minimum.checked_abs().is_err());
        assert!(minimum.to_string().starts_with('-'));
    }

    #[test]
    fn quantizes_order_size_down_without_floating_point() {
        assert_eq!(
            "1.239999"
                .parse::<Fixed>()
                .unwrap()
                .floor_to_decimals(2)
                .unwrap()
                .to_string(),
            "1.23"
        );
    }
}
