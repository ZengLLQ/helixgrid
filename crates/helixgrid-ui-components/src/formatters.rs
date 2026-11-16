use helixgrid_domain::MeasurementValue;
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

const HMS_FORMAT: &[FormatItem<'_>] = format_description!("[hour]:[minute]:[second]");
const ISO_SHORT_FORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// Formats UTC epoch milliseconds as `HH:MM:SS UTC` for dense chronological tables.
#[must_use]
pub fn format_utc_millis_hms(utc_ms: i64) -> String {
    let nanos = i128::from(utc_ms) * 1_000_000;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|dt| dt.format(HMS_FORMAT).ok())
        .map(|s| format!("{s} UTC"))
        .unwrap_or_else(|| "Unavailable".to_string())
}

/// Formats UTC epoch milliseconds as `YYYY-MM-DD HH:MM:SS UTC`.
#[must_use]
pub fn format_utc_millis_iso(utc_ms: i64) -> String {
    let nanos = i128::from(utc_ms) * 1_000_000;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|dt| dt.format(ISO_SHORT_FORMAT).ok())
        .map(|s| format!("{s} UTC"))
        .unwrap_or_else(|| "Unavailable".to_string())
}

/// Formats byte sizes with consistent tabular decimal precision.
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.1} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Formats elapsed duration cleanly for tables and inspector panels.
#[must_use]
pub fn format_duration_ms(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.2} s", ms as f64 / 1000.0)
    } else {
        let secs = ms / 1000;
        let mins = secs / 60;
        let rem_s = secs % 60;
        format!("{mins}m {rem_s:02}s")
    }
}

/// Formats a percentage measurement, explicitly reporting "Unavailable" when unmeasured.
#[must_use]
pub fn format_measurement_pct(val: &MeasurementValue<f64>) -> String {
    match val {
        MeasurementValue::Current(v) => format!("{v:.1}%"),
        MeasurementValue::Historical(v) => format!("{v:.1}% (hist)"),
        MeasurementValue::Estimate(v) => format!("~{v:.1}%"),
        MeasurementValue::Unavailable { .. } => "Unavailable".to_string(),
    }
}

/// Shortens a UUID string consistently without switching to monospace.
#[must_use]
pub fn format_short_id(id: &impl ToString) -> String {
    let s = id.to_string();
    s.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_units_cleanly() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024 * 1024 * 256), "256.0 MiB");
        assert_eq!(format_duration_ms(420), "420 ms");
        assert_eq!(format_duration_ms(65_000), "1m 05s");
        assert_eq!(
            format_measurement_pct(&MeasurementValue::Unavailable { reason: "none".into() }),
            "Unavailable"
        );
    }
}
