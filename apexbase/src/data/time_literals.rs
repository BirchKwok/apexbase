//! Strict parsing for SQL time-family literals.
//!
//! The timestamp/date/time/interval family shares one 8-byte payload on disk
//! (UTC microseconds, days since the epoch, microseconds since midnight, signed
//! microseconds respectively). These parsers are the single place that turns a
//! text literal into that payload, and they fail loudly instead of silently
//! storing 0 for an unparseable value.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date")
}

/// Parse a timestamp literal: naive `YYYY-MM-DD[ HH:MM[:SS[.fff]]]`, RFC 3339
/// with an explicit offset (normalized to UTC), or raw epoch microseconds.
pub(crate) fn parse_timestamp_micros(text: &str) -> Result<i64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty timestamp literal".to_string());
    }
    if let Ok(moment) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(moment.with_timezone(&chrono::Utc).timestamp_micros());
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"] {
        if let Ok(moment) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Ok(moment.and_utc().timestamp_micros());
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Ok(date
            .and_hms_opt(0, 0, 0)
            .expect("midnight")
            .and_utc()
            .timestamp_micros());
    }
    if let Ok(raw) = trimmed.parse::<i64>() {
        return Ok(raw);
    }
    Err(format!("invalid TIMESTAMP literal: {:?}", trimmed))
}

/// Parse a TIMESTAMPTZ literal. An aware RFC 3339 value is normalized to UTC;
/// a naive value is treated as UTC (V5 has no time-zone database).
pub(crate) fn parse_timestamptz_micros(text: &str) -> Result<i64, String> {
    parse_timestamp_micros(text)
}

/// Parse a date literal: `YYYY-MM-DD` or raw days since the Unix epoch.
pub(crate) fn parse_date_days(text: &str) -> Result<i32, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty DATE literal".to_string());
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let days = (date - epoch()).num_days();
        return i32::try_from(days).map_err(|_| format!("DATE out of range: {:?}", trimmed));
    }
    if let Ok(raw) = trimmed.parse::<i64>() {
        return i32::try_from(raw).map_err(|_| format!("DATE out of range: {:?}", trimmed));
    }
    Err(format!("invalid DATE literal: {:?}", trimmed))
}

/// Parse a time-of-day literal: `HH:MM[:SS[.fff]]` or raw microseconds since
/// midnight.
pub(crate) fn parse_time_micros(text: &str) -> Result<i64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty TIME literal".to_string());
    }
    for format in ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
        if let Ok(time) = NaiveTime::parse_from_str(trimmed, format) {
            let seconds = time.num_seconds_from_midnight() as i64;
            let micros = time.nanosecond() as i64 / 1_000;
            return Ok(seconds * 1_000_000 + micros);
        }
    }
    if let Ok(raw) = trimmed.parse::<i64>() {
        return Ok(raw);
    }
    Err(format!("invalid TIME literal: {:?}", trimmed))
}

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// Parse an interval literal: `<n> <unit>` (seconds/minutes/hours/days/weeks,
/// with fractional amounts), `HH:MM[:SS[.fff]]`, or raw microseconds.
pub(crate) fn parse_interval_micros(text: &str) -> Result<i64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty INTERVAL literal".to_string());
    }
    if let Ok(raw) = trimmed.parse::<i64>() {
        return Ok(raw);
    }
    // `HH:MM[:SS[.fff]]`
    if trimmed.contains(':') {
        let mut parts = trimmed.split(':');
        let hours: i64 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| format!("invalid INTERVAL literal: {:?}", trimmed))?;
        let minutes: i64 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| format!("invalid INTERVAL literal: {:?}", trimmed))?;
        let seconds: f64 = parts
            .next()
            .map(|part| part.parse::<f64>())
            .transpose()
            .map_err(|_| format!("invalid INTERVAL literal: {:?}", trimmed))?
            .unwrap_or(0.0);
        if parts.next().is_some() || minutes >= 60 || !(0.0..60.0).contains(&seconds) {
            return Err(format!("invalid INTERVAL literal: {:?}", trimmed));
        }
        let total = hours * MICROS_PER_HOUR
            + minutes * MICROS_PER_MINUTE
            + (seconds * MICROS_PER_SECOND as f64).round() as i64;
        return Ok(total);
    }
    // `<n> <unit>` with an optional plural and optional fractional amount.
    let mut fields = trimmed.split_whitespace();
    let amount: f64 = fields
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| format!("invalid INTERVAL literal: {:?}", trimmed))?;
    let unit = fields
        .next()
        .ok_or_else(|| format!("INTERVAL literal needs a unit: {:?}", trimmed))?
        .to_ascii_lowercase();
    if fields.next().is_some() {
        return Err(format!(
            "INTERVAL literal supports one unit (e.g. '1 day'): {:?}",
            trimmed
        ));
    }
    let unit = unit.trim_end_matches('s');
    let scale = match unit {
        "microsecond" => 1.0,
        "millisecond" => 1_000.0,
        "second" | "sec" => MICROS_PER_SECOND as f64,
        "minute" | "min" => MICROS_PER_MINUTE as f64,
        "hour" | "hr" => MICROS_PER_HOUR as f64,
        "day" => MICROS_PER_DAY as f64,
        "week" => 7.0 * MICROS_PER_DAY as f64,
        _ => return Err(format!("unsupported INTERVAL unit: {:?}", unit)),
    };
    Ok((amount * scale).round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamp_forms() {
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00"), Ok(0));
        assert_eq!(parse_timestamp_micros("1970-01-02"), Ok(86_400_000_000));
        assert_eq!(
            parse_timestamp_micros("1970-01-01T00:00:01.500Z"),
            Ok(1_500_000)
        );
        assert_eq!(parse_timestamp_micros("1234"), Ok(1234));
        assert!(parse_timestamp_micros("not-a-time").is_err());
    }

    #[test]
    fn parses_date_and_time_forms() {
        assert_eq!(parse_date_days("1970-01-01"), Ok(0));
        assert_eq!(parse_date_days("1970-01-03"), Ok(2));
        assert!(parse_date_days("2024-13-01").is_err());
        assert_eq!(parse_time_micros("01:02:03"), Ok(3_723_000_000));
        assert_eq!(parse_time_micros("01:02"), Ok(3_720_000_000));
        assert!(parse_time_micros("25:00:00").is_err());
    }

    #[test]
    fn parses_interval_forms() {
        assert_eq!(parse_interval_micros("1 second"), Ok(1_000_000));
        assert_eq!(parse_interval_micros("2 days"), Ok(2 * 86_400_000_000));
        assert_eq!(parse_interval_micros("01:00:00"), Ok(3_600_000_000));
        assert_eq!(parse_interval_micros("1.5 hours"), Ok(5_400_000_000));
        assert!(parse_interval_micros("1 fortnight").is_err());
    }
}
