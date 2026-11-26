/*
 *
 *  * Copyright (c) 2026 Couchbase, Inc.
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

//! Conversational Query (RFC 86) statements and error classification. Classification is kept
//! separate from normal Query error because the RFC deviates its error-handling.

use crate::queryx::error::{ErrorDesc, ErrorKind as QueryErrorKind, ServerError};
use serde_json::{Map, Value};
use std::time::Duration;

pub const ERR_NL_TOO_MANY_WAITING: u32 = 19220;
pub const ERR_NL_ADMISSION_TIMEOUT: u32 = 19221;
pub const ERR_NL_DISABLED: u32 = 19222;
pub const ERR_CHAT_NOT_LIVE: u32 = 19236;
pub const ERR_CHAT_NO_SLOTS: u32 = 19240;
pub const ERR_CHAT_OWNERSHIP: u32 = 19241;
pub const ERR_CHAT_RESUME_FAILED: u32 = 19243;
pub const ERR_CHAT_ALREADY_LIVE: u32 = 19255;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConversationalErrorClass {
    RateLimited,
    FeatureNotAvailable,
    ChatNotLive,
    AuthenticationFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversationalError {
    pub code: u32,
    // `None` when the SDK's normal handling for the error applies.
    pub class: Option<ConversationalErrorClass>,
}

pub fn classify(err: &ServerError) -> ConversationalError {
    let first = err.all_error_descs().first();
    let code = first.map(ErrorDesc::code).unwrap_or_else(|| err.code());

    let class = match code {
        ERR_NL_TOO_MANY_WAITING | ERR_NL_ADMISSION_TIMEOUT | ERR_CHAT_NO_SLOTS => {
            Some(ConversationalErrorClass::RateLimited)
        }
        ERR_NL_DISABLED => Some(ConversationalErrorClass::FeatureNotAvailable),
        ERR_CHAT_NOT_LIVE => Some(ConversationalErrorClass::ChatNotLive),
        ERR_CHAT_OWNERSHIP => Some(ConversationalErrorClass::AuthenticationFailure),
        ERR_CHAT_RESUME_FAILED
            if first.and_then(ErrorDesc::reason).and_then(reason_code)
                == Some(ERR_CHAT_NO_SLOTS) =>
        {
            Some(ConversationalErrorClass::RateLimited)
        }
        _ => None,
    };

    ConversationalError { code, class }
}

/// Classifies `err` if it is a Query server error, using its first error description.
pub fn classify_error(err: &crate::error::Error) -> Option<ConversationalError> {
    let crate::error::ErrorKind::Query(e) = err.kind() else {
        return None;
    };
    match e.kind() {
        QueryErrorKind::Server(e) => Some(classify(e)),
        QueryErrorKind::Resource(e) => Some(classify(e.cause())),
        _ => None,
    }
}

fn reason_code(reason: &Value) -> Option<u32> {
    reason
        .get("code")
        .and_then(Value::as_u64)
        .and_then(|code| u32::try_from(code).ok())
}

pub(crate) fn keyspace_path(bucket: &str, scope: &str, collection: &str) -> String {
    format!("`{bucket}`.`{scope}`.`{collection}`")
}

pub(crate) fn insert_generation_fields(
    fields: &mut Map<String, Value>,
    hint: Option<String>,
    execute: Option<bool>,
    output: Option<&str>,
) {
    if let Some(hint) = hint {
        fields.insert("hint".into(), Value::String(hint));
    }
    if let Some(execute) = execute {
        fields.insert("execute".into(), Value::Bool(execute));
    }
    if let Some(output) = output {
        fields.insert("output".into(), Value::from(output));
    }
}

pub(crate) fn using_ai_statement(fields: Map<String, Value>, prompt: &str) -> String {
    format!("USING AI WITH {} {prompt}", Value::Object(fields))
}

pub(crate) fn begin_chat_statement(fields: Map<String, Value>) -> String {
    format!("BEGIN CHAT WITH {}", Value::Object(fields))
}

pub(crate) fn pause_chat_statement(summarize: Option<bool>) -> String {
    match summarize {
        Some(summarize) => format!(
            "PAUSE CHAT WITH {}",
            serde_json::json!({ "summarize": summarize })
        ),
        None => "PAUSE CHAT".to_string(),
    }
}

pub(crate) const RESUME_CHAT_STATEMENT: &str = "RESUME CHAT";
pub(crate) const END_CHAT_STATEMENT: &str = "END CHAT";

pub(crate) fn whole_seconds(duration: Duration) -> u64 {
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queryx::error::ServerErrorKind;
    use http::StatusCode;
    use serde_json::json;

    fn server_error(descs: Vec<(u32, bool, Option<Value>)>) -> ServerError {
        let descs: Vec<ErrorDesc> = descs
            .into_iter()
            .map(|(code, retry, reason)| {
                ErrorDesc::new(ServerErrorKind::Unknown, code, "msg".into(), retry, reason)
            })
            .collect();
        let chosen = descs.iter().find(|d| !d.retry()).unwrap_or(&descs[0]);
        ServerError::new(
            ServerErrorKind::Unknown,
            "endpoint",
            StatusCode::OK,
            chosen.code(),
            chosen.retry(),
            "msg",
        )
        .with_error_descs(descs)
    }

    fn classify_one(code: u32, reason: Option<Value>) -> ConversationalError {
        classify(&server_error(vec![(code, false, reason)]))
    }

    #[test]
    fn classifies_mapped_codes() {
        use ConversationalErrorClass::*;
        for (code, class) in [
            (19220, RateLimited),
            (19221, RateLimited),
            (19222, FeatureNotAvailable),
            (19236, ChatNotLive),
            (19240, RateLimited),
            (19241, AuthenticationFailure),
        ] {
            assert_eq!(
                ConversationalError {
                    code,
                    class: Some(class)
                },
                classify_one(code, None)
            );
        }
    }

    #[test]
    fn resume_failed_is_unmapped_unless_slots_exhausted() {
        let plain = classify_one(19243, Some(json!("chat not found")));
        assert_eq!(19243, plain.code);
        assert_eq!(None, plain.class);

        let other_nested = classify_one(19243, Some(json!({"code": 12345, "msg": "x"})));
        assert_eq!(None, other_nested.class);

        let no_slots = classify_one(19243, Some(json!({"code": 19240, "msg": "no slots"})));
        assert_eq!(19243, no_slots.code);
        assert_eq!(Some(ConversationalErrorClass::RateLimited), no_slots.class);
    }

    #[test]
    fn already_live_is_unmapped() {
        assert_eq!(
            ConversationalError {
                code: 19255,
                class: None
            },
            classify_one(19255, None)
        );
    }

    #[test]
    fn first_top_level_error_wins() {
        // The reader would choose 19236 (first non-retryable); the RFC requires the first error.
        let err = server_error(vec![(19255, true, None), (19236, false, None)]);
        assert_eq!(19236, err.code());

        let classified = classify(&err);
        assert_eq!(19255, classified.code);
        assert_eq!(None, classified.class);
    }

    #[test]
    fn statements() {
        assert_eq!(
            "`travel-sample`.`inventory`.`hotel`",
            keyspace_path("travel-sample", "inventory", "hotel")
        );
        assert_eq!(
            "USING AI WITH {} Find hotels",
            using_ai_statement(Map::new(), "Find hotels")
        );
        assert_eq!("PAUSE CHAT", pause_chat_statement(None));
        assert_eq!(
            r#"PAUSE CHAT WITH {"summarize":false}"#,
            pause_chat_statement(Some(false))
        );
    }

    #[test]
    fn inactivity_timeout_rounds_up_to_whole_seconds() {
        assert_eq!(0, whole_seconds(Duration::ZERO));
        assert_eq!(30, whole_seconds(Duration::from_secs(30)));
        assert_eq!(31, whole_seconds(Duration::from_millis(30_001)));
    }

    #[test]
    fn unknown_code_is_unmapped() {
        assert_eq!(None, classify_one(3000, None).class);
    }
}
