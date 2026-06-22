use crate::proto::protocol::observability::attribute::Value as ProtoValue;
use crate::proto::protocol::observability::{SpanCreateRequest, SpanFinishRequest};
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::{field, Dispatch, Span};

// Attribute keys that the driver may send
#[allow(dead_code)]
const TRACE_KEY: &str = "trace.test.id";

pub struct PerformerSpan {
    pub span: Span,
    pub subscriber: Dispatch,
}

#[derive(Default)]
pub struct SpanOwner {
    spans: Mutex<HashMap<String, PerformerSpan>>,
}

impl SpanOwner {
    pub fn create_span(
        &self,
        req: &SpanCreateRequest,
        subscriber: Dispatch,
    ) -> Result<(), tonic::Status> {
        let mut spans = self.spans.lock().unwrap();

        let parent_span = if let Some(parent_id) = &req.parent_span_id {
            Some(spans.get(parent_id).ok_or_else(|| {
                tonic::Status::not_found(format!("Parent span with id {} not found", parent_id))
            })?)
        } else {
            None
        };

        let span = tracing::dispatcher::with_default(&subscriber, || {
            let _guard = parent_span.map(|p| p.span.enter());

            // This is hacky, but tracing spans require that all the attribute keys to be known at compile time,
            // but we want to be able to set dynamic attributes based on the request.
            // So we define all the possible keys the driver sends with empty values here, and then we record them with
            // the values from the request.
            // The same applies for the span name, but we use the special "otel.name" key for that.
            // https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/#special-fields
            let span = tracing::info_span!(
                "outer",
                otel.name = &req.name.as_str(),
                trace.test.id = field::Empty,
            );

            for (key, attr) in &req.attributes {
                match &attr.value {
                    Some(ProtoValue::ValueLong(v)) => {
                        span.record(key.as_str(), v);
                    }
                    Some(ProtoValue::ValueString(v)) => {
                        span.record(key.as_str(), v.as_str());
                    }
                    Some(ProtoValue::ValueBoolean(v)) => {
                        span.record(key.as_str(), v);
                    }
                    None => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "Unexpected span attribute value type for key: {}",
                            key
                        )));
                    }
                }
            }
            Ok(span)
        })?;

        spans.insert(req.id.clone(), PerformerSpan { span, subscriber });
        Ok(())
    }

    pub fn finish_span(&self, req: &SpanFinishRequest) -> Result<(), tonic::Status> {
        let mut spans = self.spans.lock().unwrap();
        let performer_span = spans.remove(&req.id).ok_or_else(|| {
            tonic::Status::not_found(format!("Span with id {} not found", req.id))
        })?;

        tracing::dispatcher::with_default(&performer_span.subscriber, || {
            drop(performer_span.span);
        });

        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<Span> {
        let spans = self.spans.lock().unwrap();
        spans.get(id).map(|s| s.span.clone())
    }
}
