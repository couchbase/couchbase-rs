/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

//! Periodic operation-metrics logging via a [`tracing`] layer.
//!
//! [`LoggingMeter`] collects operation latency histograms and emits a JSON summary
//! at a configurable interval. Register it as a layer on a `tracing_subscriber::Registry`
//! to enable metrics logging.

use couchbase_core::log_redaction::not_sensitive;
use hdrhistogram::Histogram;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::{Duration, Instant};

struct OpHistogram {
    histogram: Histogram<u64>,
    total_count: u64,
}

impl OpHistogram {
    fn new() -> Self {
        Self {
            histogram: Histogram::new(3).unwrap(),
            total_count: 0,
        }
    }

    fn record(&mut self, value: u64) {
        let _ = self.histogram.record(value);
        self.total_count += 1;
    }
}

type Service = String;
type Operation = String;

type MetricsMap = HashMap<Service, HashMap<Operation, OpHistogram>>;

use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

#[derive(Default)]
struct MetricEventVisitor {
    service: Option<String>,
    operation: Option<String>,
    duration_us: Option<u64>,
}

impl Visit for MetricEventVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "histogram.db.client.operation.duration"
            && value.is_finite()
            && value >= 0.0
        {
            let micros = (value * 1_000_000.0) as u64;
            self.duration_us = Some(micros);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "couchbase.service" => self.service = Some(value.to_string()),
            "db.operation.name" => self.operation = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {
        // do nothing
    }
}

struct MetricEvent {
    service: String,
    operation: String,
    duration_us: u64,
}

/// A `tracing` [`Layer`] that collects operation duration metrics
/// and periodically emits them as a JSON log message with percentile information.
///
/// Enable this layer by adding it to your `tracing_subscriber` stack. Metrics are emitted
/// at a configurable interval (default: 600 seconds).
///
/// If no other meters are enabled then it is **strongly** advised that you enable this by default
/// to aid with debugging.
///
/// **Stability: Uncommitted** — This API may change in the future.
/// As the `tracing` crate itself is unstable this API is subject to changes.
///
/// # Example
///
/// ```rust,no_run
/// use couchbase::logging_meter::{LoggingMeter, LoggingMeterOptions};
/// use tracing_subscriber::layer::SubscriberExt;
/// use std::time::Duration;
///
/// # fn example() {
/// let meter = LoggingMeter::new(
///     Some(LoggingMeterOptions::new().emit_interval(Duration::from_secs(300)))
/// );
/// let subscriber = tracing_subscriber::registry().with(meter);
/// // tracing::subscriber::set_global_default(subscriber).unwrap();
/// # }
/// ```
pub struct LoggingMeter {
    sender: UnboundedSender<MetricEvent>,
}

impl LoggingMeter {
    /// Creates a new `LoggingMeter` with the given options.
    ///
    /// Must be called within a Tokio runtime (panics otherwise).
    pub fn new(options: Option<LoggingMeterOptions>) -> Self {
        let options = options.unwrap_or_default();
        let emit_interval = options.emit_interval.unwrap_or(Duration::from_secs(600));

        let (tx, rx) = unbounded_channel();

        tokio::runtime::Handle::try_current()
            .expect("LoggingMeter::new must be called within a Tokio runtime.")
            .spawn(Self::logger_task(rx, emit_interval));

        Self { sender: tx }
    }

    async fn logger_task(mut receiver: UnboundedReceiver<MetricEvent>, emit_interval: Duration) {
        let mut metrics: MetricsMap = HashMap::new();
        let mut deadline = Instant::now() + emit_interval;

        // Wait for events until the deadline. When the deadline elapses, emit
        // the collected metrics and reset. Using timeout_at rather than select!
        // avoids a race where both the timer and recv are ready simultaneously
        // and the timer branch could discard buffered events.
        loop {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(event)) => {
                    let svc = metrics.entry(event.service).or_default();
                    let op = svc.entry(event.operation).or_insert_with(OpHistogram::new);
                    op.record(event.duration_us);
                }
                Ok(None) => break, // Channel closed, shut down.
                Err(_) => {
                    // Deadline elapsed — emit and start a new window.
                    Self::emit(&metrics, emit_interval);
                    metrics.clear();
                    deadline = Instant::now() + emit_interval;
                }
            }
        }
    }

    fn emit(metrics: &MetricsMap, emit_interval: Duration) {
        if metrics.is_empty() {
            return;
        }

        let operations = metrics
            .iter()
            .map(|(service, ops)| {
                let ops_json = ops
                    .iter()
                    .map(|(op, hist)| {
                        let h = &hist.histogram;

                        (
                            op.clone(),
                            json!({
                                "total_count": hist.total_count,
                                "percentiles_us": {
                                    "50.0":  h.value_at_quantile(0.50),
                                    "90.0":  h.value_at_quantile(0.90),
                                    "99.0":  h.value_at_quantile(0.99),
                                    "99.9":  h.value_at_quantile(0.999),
                                    "100.0": h.max(),
                                }
                            }),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();

                (service.clone(), json!(ops_json))
            })
            .collect::<serde_json::Map<_, _>>();

        let output = json!({
            "meta": {
                "emit_interval_s": emit_interval.as_secs(),
            },
            "operations": operations,
        });

        match serde_json::to_string(&output) {
            // The report is keyed on service and operation names and holds only counts and
            // durations, so nothing in it is sensitive. If bucket, scope or collection names
            // are ever added, tag the report as metadata.
            Ok(s) => tracing::info!("LoggingMeter {}", not_sensitive(&s)),
            Err(_) => tracing::error!("Failed to serialize LoggingMeter output"),
        }
    }
}

impl<S> Layer<S> for LoggingMeter
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "couchbase::metrics" {
            return;
        }

        let mut visitor = MetricEventVisitor::default();
        event.record(&mut visitor);

        let (service, operation, duration) =
            match (visitor.service, visitor.operation, visitor.duration_us) {
                (Some(service), Some(op), Some(dur)) => (service, op, dur),
                _ => return,
            };

        let _ = self.sender.send(MetricEvent {
            service,
            operation,
            duration_us: duration,
        });
    }
}

/// Configuration options for [`LoggingMeter`].
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct LoggingMeterOptions {
    /// How often to emit metrics. Defaults to 600 seconds (10 minutes).
    pub emit_interval: Option<Duration>,
}

impl LoggingMeterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emit_interval(mut self, interval: Duration) -> Self {
        self.emit_interval = Some(interval);
        self
    }
}

#[cfg(test)]
impl LoggingMeter {
    /// Creates a layer that writes directly to `sender`, bypassing the background
    /// logger task so tests can `try_recv` and assert on the `MetricEvent` produced.
    fn new_with_sender(sender: UnboundedSender<MetricEvent>) -> Self {
        Self { sender }
    }

    /// Returns true if the background task's receiver has been dropped (channel closed).
    /// Used to verify that `new()` successfully started a background consumer.
    fn sender_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::unbounded_channel;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn emit_outputs_json_with_correct_structure() {
        let mut metrics: MetricsMap = HashMap::new();
        let svc = metrics.entry("kv".to_string()).or_default();
        let op = svc
            .entry("get".to_string())
            .or_insert_with(OpHistogram::new);
        op.record(1_000);
        op.record(2_000);
        op.record(3_000);

        let writer = CaptureWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            LoggingMeter::emit(&metrics, Duration::from_secs(600));
        });

        let output = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("LoggingMeter"),
            "output should contain 'LoggingMeter', got: {output}"
        );
        assert!(
            output.contains("\"kv\""),
            "service name should appear in output"
        );
        assert!(
            output.contains("\"get\""),
            "operation name should appear in output"
        );
        assert!(
            output.contains("total_count"),
            "total_count should appear in output"
        );
        assert!(
            output.contains("percentiles_us"),
            "percentiles_us should appear in output"
        );
        assert!(
            output.contains("\"total_count\":3"),
            "total_count of 3 records should appear in output"
        );
        assert!(
            output.contains("\"emit_interval_s\":600"),
            "emit_interval_s should appear in meta section"
        );
    }

    #[test]
    fn emit_does_not_log_when_metrics_empty() {
        let metrics: MetricsMap = HashMap::new();

        let writer = CaptureWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            LoggingMeter::emit(&metrics, Duration::from_secs(600));
        });

        let output = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.is_empty(),
            "no output should be emitted when metrics is empty, got: {output}"
        );
    }

    #[tokio::test]
    async fn layer_sends_correct_metric_event_fields() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "couchbase::metrics",
                tracing::Level::TRACE,
                histogram.db.client.operation.duration = 0.001f64, // 1 ms
                couchbase.service = "kv",
                db.operation.name = "get",
            );
        });

        let event = rx
            .try_recv()
            .expect("metric event should have been forwarded");
        assert_eq!(event.service, "kv");
        assert_eq!(event.operation, "get");
        assert_eq!(
            event.duration_us, 1_000,
            "0.001 s should convert to 1000 µs"
        );
    }

    #[tokio::test]
    async fn layer_converts_seconds_to_microseconds_correctly() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "couchbase::metrics",
                tracing::Level::TRACE,
                histogram.db.client.operation.duration = 1.5f64, // 1.5 s
                couchbase.service = "kv",
                db.operation.name = "get",
            );
        });

        let event = rx
            .try_recv()
            .expect("metric event should have been forwarded");
        assert_eq!(
            event.duration_us, 1_500_000,
            "1.5 s should convert to 1,500,000 µs"
        );
    }

    #[tokio::test]
    async fn layer_ignores_events_with_wrong_target() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "some_other_target",
                tracing::Level::TRACE,
                histogram.db.client.operation.duration = 0.001f64,
                couchbase.service = "kv",
                db.operation.name = "get",
            );
        });

        assert!(
            rx.try_recv().is_err(),
            "event with wrong target should not be forwarded"
        );
    }

    #[tokio::test]
    async fn layer_ignores_event_with_missing_duration() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "couchbase::metrics",
                tracing::Level::TRACE,
                couchbase.service = "kv",
                db.operation.name = "get",
            );
        });

        assert!(
            rx.try_recv().is_err(),
            "event missing duration should not be forwarded"
        );
    }

    #[tokio::test]
    async fn layer_ignores_event_with_missing_service() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "couchbase::metrics",
                tracing::Level::TRACE,
                histogram.db.client.operation.duration = 0.001f64,
                db.operation.name = "get",
            );
        });

        assert!(
            rx.try_recv().is_err(),
            "event missing service should not be forwarded"
        );
    }

    #[tokio::test]
    async fn layer_processes_multiple_services_and_operations() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            for _ in 0..5 {
                tracing::event!(
                    target: "couchbase::metrics",
                    tracing::Level::TRACE,
                    histogram.db.client.operation.duration = 0.001f64,
                    couchbase.service = "kv",
                    db.operation.name = "get",
                );
            }
            for _ in 0..3 {
                tracing::event!(
                    target: "couchbase::metrics",
                    tracing::Level::TRACE,
                    histogram.db.client.operation.duration = 0.5f64,
                    couchbase.service = "query",
                    db.operation.name = "query",
                );
            }
        });

        let mut kv_count = 0;
        let mut query_count = 0;
        while let Ok(event) = rx.try_recv() {
            match event.service.as_str() {
                "kv" => {
                    assert_eq!(event.operation, "get");
                    assert_eq!(event.duration_us, 1_000);
                    kv_count += 1;
                }
                "query" => {
                    assert_eq!(event.operation, "query");
                    assert_eq!(event.duration_us, 500_000);
                    query_count += 1;
                }
                s => panic!("unexpected service: {s}"),
            }
        }
        assert_eq!(kv_count, 5, "expected 5 kv events");
        assert_eq!(query_count, 3, "expected 3 query events");
    }

    #[tokio::test]
    async fn layer_ignores_negative_duration() {
        let (tx, mut rx) = unbounded_channel::<MetricEvent>();
        let layer = LoggingMeter::new_with_sender(tx);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "couchbase::metrics",
                tracing::Level::TRACE,
                histogram.db.client.operation.duration = -1.0f64,
                couchbase.service = "kv",
                db.operation.name = "get",
            );
        });

        assert!(
            rx.try_recv().is_err(),
            "event with negative duration should not be forwarded"
        );
    }

    #[test]
    #[should_panic(expected = "LoggingMeter::new must be called within a Tokio runtime")]
    fn new_outside_tokio_runtime_panics() {
        LoggingMeter::new(None);
    }
}
