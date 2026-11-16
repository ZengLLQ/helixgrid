use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeriesPoint {
    pub value: f64,
}

/// Generates SVG-style path commands (`M x y L x y ...`) normalized to `0..=width` and `0..=height`
/// for Slint's `Path { commands: ... }` vector chart renderer.
/// Returns an empty string when fewer than 2 real points exist (never fabricates chart lines).
#[must_use]
pub fn build_line_path_commands(
    values: &[f64],
    width: f32,
    height: f32,
    min_val: f64,
    max_val: f64,
) -> String {
    if values.len() < 2 || width <= 4.0 || height <= 4.0 {
        return String::new();
    }
    let span = (max_val - min_val).max(1e-6);
    let pad_y = 4.0_f32;
    let usable_h = (height - pad_y * 2.0).max(1.0);
    let step_x = width / ((values.len() - 1) as f32);

    let mut out = String::with_capacity(values.len() * 18);
    for (idx, val) in values.iter().enumerate() {
        let clamped = val.clamp(min_val, max_val);
        let norm = ((clamped - min_val) / span) as f32;
        let x = (idx as f32) * step_x;
        let y = pad_y + usable_h * (1.0 - norm);
        if idx == 0 {
            let _ = write!(&mut out, "M {x:.2} {y:.2}");
        } else {
            let _ = write!(&mut out, " L {x:.2} {y:.2}");
        }
    }
    out
}

/// Generates a closed SVG-style area path (`M 0 bottom L ... L width bottom Z`) for subtle under-line fill.
#[must_use]
pub fn build_area_path_commands(
    values: &[f64],
    width: f32,
    height: f32,
    min_val: f64,
    max_val: f64,
) -> String {
    let line = build_line_path_commands(values, width, height, min_val, max_val);
    if line.is_empty() {
        return String::new();
    }
    format!("M 0.00 {height:.2} L {} L {width:.2} {height:.2} Z", &line[2..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_path_requires_real_points() {
        assert!(build_line_path_commands(&[], 200.0, 80.0, 0.0, 100.0).is_empty());
        assert!(build_line_path_commands(&[10.0], 200.0, 80.0, 0.0, 100.0).is_empty());
        let p = build_line_path_commands(&[0.0, 50.0, 100.0], 200.0, 100.0, 0.0, 100.0);
        assert!(p.starts_with("M 0.00 "));
        assert!(p.contains("L 100.00 "));
    }
}
