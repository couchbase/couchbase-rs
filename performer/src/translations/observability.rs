use crate::proto::protocol::observability::attribute::Value;
use crate::proto::protocol::observability::{Attribute, Config, MetricsConfig, TracingConfig};
use couchbase::logging_meter::LoggingMeter;
use couchbase::threshold_logging_tracer::{ThresholdLoggingOptions, ThresholdLoggingTracer};
use log::warn;
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::trace::SimpleSpanProcessor;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use std::future::Future;
use std::time::Duration;
use tracing::Dispatch;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[derive(Debug)]
struct DebugExporter;

#[derive(Debug)]
struct DebugMetricExporter;

impl PushMetricExporter for DebugMetricExporter {
    fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
        eprintln!("=== EXPORTED METRICS ===");
        eprintln!("  metrics: {:?}", metrics);
        std::future::ready(Ok(()))
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

impl SpanExporter for DebugExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        for span in &batch {
            eprintln!("=== EXPORTED SPAN ===");
            eprintln!("  name: {}", span.name);
            eprintln!("  trace_id: {:?}", span.span_context.trace_id());
            eprintln!("  span_id: {:?}", span.span_context.span_id());
            eprintln!("  parent_span_id: {:?}", span.parent_span_id);
            eprintln!("  start: {:?}", span.start_time);
            eprintln!("  end: {:?}", span.end_time);
            eprintln!("  attributes: {:?}", span.attributes);
            eprintln!("  status: {:?}", span.status);
        }
        std::future::ready(Ok(()))
    }
}

pub fn tracing_subscriber(config: Option<&Config>) -> Dispatch {
    let registry: Registry = tracing_subscriber::registry();
    let Some(config) = config else {
        return Dispatch::new(registry);
    };

    if config.use_noop_tracer {
        // Do nothing
    }

    let threshold_layer = config.threshold_logging_tracer.map(|threshold_config| {
        let mut opts = ThresholdLoggingOptions::new();

        if let Some(emit_interval) = threshold_config.emit_interval_millis {
            opts = opts.emit_interval(Duration::from_millis(emit_interval as u64));
        }
        if let Some(kv_threshold) = threshold_config.kv_threshold_millis {
            opts = opts.kv_threshold(Duration::from_millis(kv_threshold as u64));
        }
        if let Some(query_threshold) = threshold_config.query_threshold_millis {
            opts = opts.query_threshold(Duration::from_millis(query_threshold as u64));
        }
        if let Some(search_threshold) = threshold_config.search_threshold_millis {
            opts = opts.search_threshold(Duration::from_millis(search_threshold as u64));
        }
        if threshold_config.views_threshold_millis.is_some() {
            warn!("views_threshold_millis not supported - ignoring");
        }
        if threshold_config.analytics_threshold_millis.is_some() {
            warn!("analytics_threshold_millis not supported - ignoring");
        }
        if threshold_config.transactions_threshold_millis.is_some() {
            warn!("transactions_threshold_millis not supported - ignoring");
        }
        if let Some(sample_size) = threshold_config.sample_size {
            opts = opts.sample_size(sample_size as usize);
        }
        ThresholdLoggingTracer::new(Some(opts))
    });

    let logging_meter_layer = config.logging_meter.map(|logging_meter_config| {
        let mut opts = couchbase::logging_meter::LoggingMeterOptions::new();

        if let Some(emit_interval) = logging_meter_config.emit_interval_millis {
            opts = opts.emit_interval(Duration::from_millis(emit_interval as u64));
        }

        LoggingMeter::new(Some(opts))
    });

    let tracing_otel_layer = config.tracing.as_ref().map(|tracing_config| {
        let provider = create_tracer_provider(tracing_config);
        let tracer = provider.tracer("com.couchbase.client/rust");
        tracing_opentelemetry::layer().with_tracer(tracer)
    });

    let metrics_otel_layer = config.metrics.as_ref().map(|metrics_config| {
        let provider = create_meter_provider(metrics_config);
        tracing_opentelemetry::MetricsLayer::new(provider.clone())
    });

    let subscriber = registry
        .with(threshold_layer)
        .with(logging_meter_layer)
        .with(tracing_otel_layer)
        .with(metrics_otel_layer);
    Dispatch::new(subscriber)
}

fn create_otel_resource(resources: &std::collections::HashMap<String, Attribute>) -> Resource {
    let kvs: Vec<KeyValue> = resources
        .iter()
        .filter_map(|(k, v)| {
            let value = match &v.value {
                Some(Value::ValueString(s)) => opentelemetry::Value::String(s.clone().into()),
                Some(Value::ValueLong(l)) => opentelemetry::Value::I64(*l),
                Some(Value::ValueBoolean(b)) => opentelemetry::Value::Bool(*b),
                _ => {
                    warn!(
                        "Unexpected OpenTelemetry resource attribute value for key: {}",
                        k
                    );
                    return None;
                }
            };
            Some(KeyValue::new(k.clone(), value))
        })
        .collect();

    Resource::builder().with_attributes(kvs).build()
}

fn create_tracer_provider(cfg: &TracingConfig) -> SdkTracerProvider {
    let exporter = {
        let builder = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(cfg.endpoint_hostname.clone())
            .with_tls_config(
                opentelemetry_otlp::tonic_types::transport::ClientTlsConfig::new()
                    .with_native_roots(),
            );

        builder.build().expect("Failed to build OTLP span exporter")
    };

    let resource = create_otel_resource(&cfg.resources);

    let sampler = {
        let epsilon = 0.00001_f64;
        let pct = cfg.sampling_percentage as f64;
        if pct < epsilon {
            Sampler::AlwaysOff
        } else if pct > 1.0 - epsilon {
            Sampler::AlwaysOn
        } else {
            Sampler::TraceIdRatioBased(pct)
        }
    };

    let mut builder = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(sampler);

    if cfg.batching {
        let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                opentelemetry_sdk::trace::BatchConfigBuilder::default()
                    .with_scheduled_delay(Duration::from_millis(cfg.export_every_millis as u64))
                    .build(),
            )
            .build();
        builder = builder.with_span_processor(processor);
    } else {
        let processor = SimpleSpanProcessor::new(exporter);
        builder = builder.with_span_processor(processor);
    }

    // Uncomment for debugging spans:
    // builder = builder.with_span_processor(SimpleSpanProcessor::new(DebugExporter));

    builder.build()
}

pub fn create_meter_provider(cfg: &MetricsConfig) -> SdkMeterProvider {
    let exporter = {
        let builder = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(cfg.endpoint_hostname.clone())
            .with_tls_config(
                opentelemetry_otlp::tonic_types::transport::ClientTlsConfig::new()
                    .with_native_roots(),
            );
        builder
            .build()
            .expect("Failed to build OTLP metric exporter")
    };

    let export_interval = Duration::from_millis(cfg.export_every_millis as u64);

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(export_interval)
        .build();

    let resource = create_otel_resource(&cfg.resources);

    // Workaround: set the unit for metrics emitted by the SDK via a View.
    // The SDK emits `otel.unit = "s"` on its tracing events, but upstream
    // tracing-opentelemetry's MetricsLayer does not yet support the otel.unit
    // field (see https://github.com/tokio-rs/tracing-opentelemetry/pull/255).
    // Using a View on the MeterProvider allows us to set the unit at the
    // application level until that PR is merged.
    let unit_view = |inst: &opentelemetry_sdk::metrics::Instrument| -> Option<opentelemetry_sdk::metrics::Stream> {
        match inst.name() {
            "db.client.operation.duration" => {
                opentelemetry_sdk::metrics::Stream::builder()
                    .with_unit("s")
                    .build()
                    .ok()
            }
            _ => None,
        }
    };

    let mut meter_provider_builder = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .with_view(unit_view);

    // Uncomment for debugging metrics:
    // let debug_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(DebugMetricExporter)
    //     .with_interval(export_interval)
    //     .build();
    // meter_provider_builder = meter_provider_builder.with_reader(debug_reader);

    meter_provider_builder.build()
}
