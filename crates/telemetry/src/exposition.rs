//! The Prometheus text format, as far as it is needed: counters, gauges and histograms with
//! labels. Writing it is for scrapes, off the request path, and takes its time and memory.
//!
//! Metric and label names come from the code and are written as they are; label values come
//! from configs (the names of listeners and upstreams) and are escaped.

use std::fmt::{Display, Write};

/// What kind of metric a family is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A number that only grows.
    Counter,
    /// A number that goes up and down.
    Gauge,
    /// Counts of values by range.
    Histogram,
}

/// A scrape being written: for every family its description, then its samples.
#[derive(Debug, Default)]
pub struct Exposition {
    text: String,
}

impl Exposition {
    /// An empty one.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Describes a family; its samples follow.
    pub fn family(&mut self, name: &str, kind: Kind, help: &str) {
        let kind = match kind {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
        };
        self.text.push_str("# HELP ");
        self.text.push_str(name);
        self.text.push(' ');
        for character in help.chars() {
            match character {
                '\\' => self.text.push_str("\\\\"),
                '\n' => self.text.push_str("\\n"),
                other => self.text.push(other),
            }
        }
        self.line(format_args!("\n# TYPE {name} {kind}"));
    }

    /// One sample of a counter or a gauge.
    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl Display) {
        self.text.push_str(name);
        self.labels(labels, None);
        self.line(format_args!(" {value}"));
    }

    /// One histogram: `counts` as [`Histogram::counts`](crate::Histogram::counts) gives
    /// them — not cumulative, one per bound and one more for what lies beyond — and
    /// `bounds` and `sum` in the unit the metric is named after.
    pub fn histogram(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        bounds: impl IntoIterator<Item = f64>,
        counts: impl IntoIterator<Item = u64>,
        sum: f64,
    ) {
        let mut bounds = bounds.into_iter();
        let mut total = 0_u64;
        for count in counts {
            total = total.wrapping_add(count);
            let bound = bounds
                .next()
                .map_or_else(|| "+Inf".to_owned(), |bound| bound.to_string());
            self.text.push_str(name);
            self.text.push_str("_bucket");
            self.labels(labels, Some(&bound));
            self.line(format_args!(" {total}"));
        }
        self.text.push_str(name);
        self.text.push_str("_sum");
        self.labels(labels, None);
        self.line(format_args!(" {sum}"));
        self.text.push_str(name);
        self.text.push_str("_count");
        self.labels(labels, None);
        self.line(format_args!(" {total}"));
    }

    /// The text of the scrape.
    #[must_use]
    pub fn finish(self) -> String {
        self.text
    }

    fn labels(&mut self, labels: &[(&str, &str)], le: Option<&str>) {
        let labels = labels.iter().copied().chain(le.map(|le| ("le", le)));
        let mut any = false;
        for (name, value) in labels {
            self.text.push(if any { ',' } else { '{' });
            any = true;
            self.text.push_str(name);
            self.text.push_str("=\"");
            for character in value.chars() {
                match character {
                    '\\' => self.text.push_str("\\\\"),
                    '"' => self.text.push_str("\\\""),
                    '\n' => self.text.push_str("\\n"),
                    other => self.text.push(other),
                }
            }
            self.text.push('"');
        }
        if any {
            self.text.push('}');
        }
    }

    fn line(&mut self, rest: std::fmt::Arguments<'_>) {
        // Writing to a `String` cannot fail.
        let _infallible = self.text.write_fmt(rest);
        self.text.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Histogram;

    #[test]
    fn a_scrape_is_families_with_their_samples() {
        let mut scrape = Exposition::new();
        scrape.family("requests_total", Kind::Counter, "Requests that came in.");
        scrape.sample(
            "requests_total",
            &[("listener", "web"), ("class", "2xx")],
            1_234_u64,
        );
        scrape.sample(
            "requests_total",
            &[("listener", "web"), ("class", "5xx")],
            0_u64,
        );
        scrape.family(
            "connections_active",
            Kind::Gauge,
            "Connections that are open.",
        );
        scrape.sample("connections_active", &[], -3_i64);
        assert_eq!(
            scrape.finish(),
            "# HELP requests_total Requests that came in.\n\
             # TYPE requests_total counter\n\
             requests_total{listener=\"web\",class=\"2xx\"} 1234\n\
             requests_total{listener=\"web\",class=\"5xx\"} 0\n\
             # HELP connections_active Connections that are open.\n\
             # TYPE connections_active gauge\n\
             connections_active -3\n"
        );
    }

    #[test]
    fn a_histogram_is_written_cumulative_with_its_sum_and_count() {
        const BOUNDS: [u64; 3] = [500_000, 1_000_000, 2_500_000_000];
        let histogram = Histogram::default();
        for nanoseconds in [400_000, 900_000, 900_000, 7_000_000_000] {
            histogram.observe(&BOUNDS, nanoseconds);
        }
        let seconds = |nanoseconds: u64| nanoseconds as f64 / 1e9;

        let mut scrape = Exposition::new();
        scrape.family("duration_seconds", Kind::Histogram, "How long it took.");
        scrape.histogram(
            "duration_seconds",
            &[("listener", "web")],
            BOUNDS.map(seconds),
            histogram.counts(),
            seconds(histogram.sum()),
        );
        assert_eq!(
            scrape.finish(),
            "# HELP duration_seconds How long it took.\n\
             # TYPE duration_seconds histogram\n\
             duration_seconds_bucket{listener=\"web\",le=\"0.0005\"} 1\n\
             duration_seconds_bucket{listener=\"web\",le=\"0.001\"} 3\n\
             duration_seconds_bucket{listener=\"web\",le=\"2.5\"} 3\n\
             duration_seconds_bucket{listener=\"web\",le=\"+Inf\"} 4\n\
             duration_seconds_sum{listener=\"web\"} 7.0022\n\
             duration_seconds_count{listener=\"web\"} 4\n"
        );
    }

    #[test]
    fn a_histogram_without_labels_still_has_its_bounds() {
        let mut scrape = Exposition::new();
        scrape.histogram("d", &[], [1.0], [2, 1], 3.5);
        assert_eq!(
            scrape.finish(),
            "d_bucket{le=\"1\"} 2\nd_bucket{le=\"+Inf\"} 3\nd_sum 3.5\nd_count 3\n"
        );
    }

    #[test]
    fn what_comes_from_a_config_cannot_break_out_of_its_quotes() {
        let mut scrape = Exposition::new();
        scrape.family("m", Kind::Counter, "one\\two\nthree");
        scrape.sample("m", &[("upstream", "a\"b\\c\nd} 9\n")], 1_u64);
        assert_eq!(
            scrape.finish(),
            "# HELP m one\\\\two\\nthree\n\
             # TYPE m counter\n\
             m{upstream=\"a\\\"b\\\\c\\nd} 9\\n\"} 1\n"
        );
    }
}
