//! Conversational Query (SDK-RFC 86): cluster.conversational_query() and ConversationalQueryChat operations.

use crate::cluster::connection::ConnectionSet;
use crate::commands::helpers::{create_run_result, current_timestamp, duration_from_millis};
use crate::commands::query::read_rows;
use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::cluster::conversational_query as proto;
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::shared::ScanConsistency;
use crate::proto::protocol::{run, sdk, shared};
use crate::translations::common::sdk_error_to_proto_run_result;
use couchbase::collection::Collection;
use couchbase::conversational_query::{ConversationalQueryChat, ConversationalQueryChatHandle};
use couchbase::options::conversational_query_options::{
    ConversationalAskOptions, ConversationalAttachChatOptions, ConversationalBeginChatOptions,
    ConversationalEndOptions, ConversationalModelCredential, ConversationalModelOptions,
    ConversationalOutput, ConversationalPauseOptions, ConversationalQueryOptions,
    ConversationalResumeOptions,
};
use couchbase::options::query_options::{ProfileMode, ReplicaLevel};
use couchbase::results::conversational_query_results::{
    ConversationalMetaData, ConversationalResult,
};
use prost_types::Timestamp;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;

pub struct ConversationalQueryCommand {
    conn: Arc<ConnectionSet>,
    operation: Operation,
    return_result: bool,
    initiated: Timestamp,
    parent_span: Option<tracing::Span>,
    timeout_override: Option<Duration>,
}

enum Operation {
    Query {
        prompt: String,
        keyspaces: Vec<Collection>,
        options: ConversationalQueryOptions,
        content_as: Option<As>,
    },
    BeginChat {
        chat_ref: String,
        keyspaces: Vec<Collection>,
        options: ConversationalBeginChatOptions,
    },
    AttachChat {
        chat_ref: String,
        handle_json: String,
        options: ConversationalAttachChatOptions,
    },
    Ask {
        chat: Arc<ConversationalQueryChat>,
        prompt: String,
        options: ConversationalAskOptions,
        content_as: Option<As>,
    },
    Pause {
        chat: Arc<ConversationalQueryChat>,
        options: ConversationalPauseOptions,
    },
    Resume {
        chat: Arc<ConversationalQueryChat>,
        options: ConversationalResumeOptions,
    },
    End {
        chat: Arc<ConversationalQueryChat>,
        options: ConversationalEndOptions,
    },
    Handle {
        chat: Arc<ConversationalQueryChat>,
    },
}

type Outcome = std::result::Result<proto::result::Result, couchbase::error::Error>;

impl ConversationalQueryCommand {
    pub fn new(
        conn: Arc<ConnectionSet>,
        command: proto::Command,
        return_result: bool,
        span_owner: &crate::observability::span_owner::SpanOwner,
    ) -> Result<Self> {
        let command = command
            .command
            .ok_or_else(|| Error::invalid_argument("Conversational query command not set"))?;

        let mut parent_span_id = None;
        let mut timeout_millis = None;

        let operation = match command {
            proto::command::Command::Query(query) => {
                let options = query.options.unwrap_or_default();
                parent_span_id = options.parent_span_id.clone();
                timeout_millis = options.timeout_millis;
                Operation::Query {
                    prompt: query.prompt,
                    keyspaces: collections(&conn, query.keyspaces),
                    options: query_options(options)?,
                    content_as: query.content_as.and_then(|c| c.r#as),
                }
            }
            proto::command::Command::BeginChat(begin) => {
                let options = begin.options.unwrap_or_default();
                parent_span_id = options.parent_span_id.clone();
                timeout_millis = options.timeout_millis;
                Operation::BeginChat {
                    chat_ref: begin.chat_ref,
                    keyspaces: collections(&conn, begin.keyspaces),
                    options: begin_chat_options(options)?,
                }
            }
            proto::command::Command::AttachChat(attach) => Operation::AttachChat {
                chat_ref: attach.chat_ref,
                handle_json: attach.handle_json,
                options: attach_chat_options(attach.options.unwrap_or_default())?,
            },
            proto::command::Command::Chat(chat_command) => {
                let chat = conn
                    .chats
                    .lock()
                    .unwrap()
                    .get(&chat_command.chat_ref)
                    .cloned()
                    .ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "No chat with chat_ref {}",
                            chat_command.chat_ref
                        ))
                    })?;
                let command = chat_command.command.ok_or_else(|| {
                    Error::invalid_argument("ConversationalQueryChat command not set")
                })?;

                match command {
                    proto::chat_command::Command::Ask(ask) => {
                        let options = ask.options.unwrap_or_default();
                        parent_span_id = options.parent_span_id.clone();
                        timeout_millis = options.timeout_millis;
                        Operation::Ask {
                            chat,
                            prompt: ask.prompt,
                            options: ask_options(options)?,
                            content_as: ask.content_as.and_then(|c| c.r#as),
                        }
                    }
                    proto::chat_command::Command::Pause(pause) => {
                        let proto_options = pause.options.unwrap_or_default();
                        parent_span_id = proto_options.parent_span_id.clone();
                        timeout_millis = proto_options.timeout_millis;
                        let mut options = ConversationalPauseOptions::new();
                        if let Some(summarize) = proto_options.summarize {
                            options = options.summarize(summarize);
                        }
                        if let Some(timeout) = timeout_millis {
                            options = options.server_timeout(duration_from_millis(timeout)?);
                        }
                        Operation::Pause { chat, options }
                    }
                    proto::chat_command::Command::Resume(resume) => {
                        let proto_options = resume.options.unwrap_or_default();
                        parent_span_id = proto_options.parent_span_id.clone();
                        timeout_millis = proto_options.timeout_millis;
                        let mut options = ConversationalResumeOptions::new();
                        if let Some(timeout) = timeout_millis {
                            options = options.server_timeout(duration_from_millis(timeout)?);
                        }
                        Operation::Resume { chat, options }
                    }
                    proto::chat_command::Command::End(end) => {
                        let proto_options = end.options.unwrap_or_default();
                        parent_span_id = proto_options.parent_span_id.clone();
                        timeout_millis = proto_options.timeout_millis;
                        let mut options = ConversationalEndOptions::new();
                        if let Some(timeout) = timeout_millis {
                            options = options.server_timeout(duration_from_millis(timeout)?);
                        }
                        Operation::End { chat, options }
                    }
                    proto::chat_command::Command::Handle(_) => Operation::Handle { chat },
                }
            }
        };

        Ok(Self {
            conn,
            operation,
            return_result,
            initiated: current_timestamp(),
            parent_span: parent_span_id.and_then(|id| span_owner.get(&id)),
            timeout_override: timeout_millis.map(duration_from_millis).transpose()?,
        })
    }

    /// The per-operation timeout override, if one was set in the request's own options.
    pub fn timeout_override(&self) -> Option<Duration> {
        self.timeout_override
    }

    pub async fn execute(self, batcher: &crate::common::batcher::Batcher) -> Result<bool> {
        let start = Instant::now();
        let parent_span = self.parent_span.clone();
        let outcome = match parent_span {
            Some(span) => self.run().instrument(span).await?,
            None => self.run().await?,
        };

        let mut top_level_result = create_run_result(self.initiated, start.elapsed());
        let success = outcome.is_ok();
        top_level_result.result = Some(match outcome {
            Ok(_) if !self.return_result => run::result::Result::Sdk(sdk::Result {
                result: Some(sdk::result::Result::Success(true)),
            }),
            Ok(result) => run::result::Result::Sdk(sdk::Result {
                result: Some(sdk::result::Result::ConversationalQueryResult(
                    proto::Result {
                        result: Some(result),
                    },
                )),
            }),
            Err(err) => sdk_error_to_proto_run_result(err),
        });

        batcher.send(top_level_result).await;
        Ok(success)
    }

    async fn run(&self) -> Result<Outcome> {
        let cq = self.conn.cluster.conversational_query();

        Ok(match &self.operation {
            Operation::Query {
                prompt,
                keyspaces,
                options,
                content_as,
            } => match cq.query(prompt.clone(), keyspaces, options.clone()).await {
                Ok(mut res) => {
                    query_result(content_as.as_ref(), &mut res, self.return_result).await?
                }
                Err(err) => Err(err),
            },
            Operation::BeginChat {
                chat_ref,
                keyspaces,
                options,
            } => match cq.begin_chat(keyspaces, options.clone()).await {
                Ok(chat) => self.store_chat(chat_ref, chat),
                Err(err) => Err(err),
            },
            Operation::AttachChat {
                chat_ref,
                handle_json,
                options,
            } => match ConversationalQueryChatHandle::from_json(handle_json)
                .and_then(|handle| cq.attach_chat(&handle, options.clone()))
            {
                Ok(chat) => self.store_chat(chat_ref, chat),
                Err(err) => Err(err),
            },
            Operation::Ask {
                chat,
                prompt,
                options,
                content_as,
            } => match chat.ask(prompt.clone(), options.clone()).await {
                Ok(mut res) => {
                    query_result(content_as.as_ref(), &mut res, self.return_result).await?
                }
                Err(err) => Err(err),
            },
            Operation::Pause { chat, options } => {
                meta_data_result(chat.pause(options.clone()).await)?
            }
            Operation::Resume { chat, options } => {
                meta_data_result(chat.resume(options.clone()).await)?
            }
            Operation::End { chat, options } => meta_data_result(chat.end(options.clone()).await)?,
            Operation::Handle { chat } => chat
                .handle()
                .map(|handle| proto::result::Result::HandleJson(handle.to_json())),
        })
    }

    fn store_chat(&self, chat_ref: &str, chat: ConversationalQueryChat) -> Outcome {
        let handle = chat.handle()?;
        self.conn
            .chats
            .lock()
            .unwrap()
            .insert(chat_ref.to_string(), Arc::new(chat));
        Ok(proto::result::Result::HandleJson(handle.to_json()))
    }
}

async fn query_result(
    content_as: Option<&As>,
    res: &mut ConversationalResult,
    return_result: bool,
) -> Result<Outcome> {
    if !return_result {
        return Ok(Ok(proto::result::Result::QueryResult(Default::default())));
    }

    // Read before the rows, as the RFC requires it to be available without iterating them.
    let generated_statement = res.generated_statement().map(str::to_string);

    let content = match read_rows(content_as, res).await? {
        Ok(content) => content,
        Err(err) => return Ok(Err(err)),
    };
    let meta_data = match res.metadata() {
        Ok(meta) => meta_data(&meta)?,
        Err(err) => return Ok(Err(err)),
    };

    Ok(Ok(proto::result::Result::QueryResult(
        proto::ConversationalQueryResult {
            content,
            generated_statement,
            meta_data: Some(meta_data),
        },
    )))
}

fn meta_data_result(res: couchbase::error::Result<ConversationalMetaData>) -> Result<Outcome> {
    Ok(match res {
        Ok(meta) => Ok(proto::result::Result::MetaData(meta_data(&meta)?)),
        Err(err) => Err(err),
    })
}

fn meta_data(meta: &ConversationalMetaData) -> Result<proto::ConversationalQueryMetaData> {
    Ok(proto::ConversationalQueryMetaData {
        request_id: meta.request_id.clone(),
        client_context_id: meta.client_context_id.clone(),
        status: sdk::query::QueryStatus::from(meta.status) as i32,
        warnings: meta.warnings.iter().cloned().map(Into::into).collect(),
        metrics: meta.metrics.as_ref().map(TryInto::try_into).transpose()?,
        profile: meta
            .profile
            .as_ref()
            .map(|raw| raw.get().as_bytes().to_vec()),
        request_tokens: meta.request_tokens.as_ref().map(token_usage),
        chat_tokens: meta.chat_tokens.as_ref().map(token_usage),
        natural_language_processing_time: meta
            .natural_language_processing_time
            .map(|d| {
                d.try_into()
                    .map_err(|e| Error::internal(format!("Duration conversion failed: {e}")))
            })
            .transpose()?,
        generated_statement_executed: meta.generated_statement_executed,
    })
}

fn token_usage(
    usage: &couchbase::results::conversational_query_results::ConversationalTokenUsage,
) -> proto::TokenUsage {
    proto::TokenUsage {
        prompt_tokens: usage.prompt_tokens.map(|n| n as i64),
        completion_tokens: usage.completion_tokens.map(|n| n as i64),
        total_tokens: usage.total_tokens.map(|n| n as i64),
    }
}

fn collections(conn: &ConnectionSet, keyspaces: Vec<shared::Collection>) -> Vec<Collection> {
    keyspaces
        .into_iter()
        .map(|c| {
            conn.cluster
                .bucket(c.bucket_name)
                .scope(c.scope_name)
                .collection(c.collection_name)
        })
        .collect()
}

fn json_values(raw: HashMap<String, String>, what: &str) -> Result<Vec<(String, Value)>> {
    raw.into_iter()
        .map(|(key, value)| {
            serde_json::from_str(&value)
                .map(|value| (key.clone(), value))
                .map_err(|e| Error::invalid_argument(format!("{what} {key} is not JSON: {e}")))
        })
        .collect()
}

fn model_options(proto: proto::ModelOptions) -> Result<ConversationalModelOptions> {
    let mut options = ConversationalModelOptions::new();
    if let Some(provider) = proto.provider {
        options = options.provider(provider);
    }
    if let Some(name) = proto.name {
        options = options.name(name);
    }
    if let Some(endpoint) = proto.endpoint {
        options = options.endpoint(endpoint);
    }
    if let Some(limit) = proto.output_token_limit {
        let limit = u32::try_from(limit)
            .map_err(|_| Error::invalid_argument(format!("Invalid output_token_limit {limit}")))?;
        options = options.output_token_limit(limit);
    }
    if let Some(moderation) = proto.moderation {
        options = options.moderation(moderation);
    }
    for (key, value) in json_values(proto.raw, "Model raw option")? {
        options = options.add_raw(key, value)?;
    }
    Ok(options)
}

fn model_credential(proto: proto::ModelCredential) -> Result<ConversationalModelCredential> {
    match proto.credential {
        Some(proto::model_credential::Credential::Capella(capella)) => {
            Ok(ConversationalModelCredential::capella(
                capella.email,
                capella.password,
                capella.organization_id,
            ))
        }
        Some(proto::model_credential::Credential::CredentialStore(name)) => {
            Ok(ConversationalModelCredential::credential_store(name))
        }
        Some(proto::model_credential::Credential::ApiKey(key)) => {
            Ok(ConversationalModelCredential::api_key(key))
        }
        None => Err(Error::invalid_argument("Model credential not set")),
    }
}

fn output(value: i32) -> Result<ConversationalOutput> {
    match proto::ConversationalQueryOutput::try_from(value) {
        Ok(proto::ConversationalQueryOutput::Sqlpp) => Ok(ConversationalOutput::Sqlpp),
        Ok(proto::ConversationalQueryOutput::FtsSqlpp) => Ok(ConversationalOutput::FtsSqlpp),
        Ok(proto::ConversationalQueryOutput::JsUdf) => Ok(ConversationalOutput::JsUdf),
        Err(_) => Err(Error::invalid_argument(format!("Invalid output {value}"))),
    }
}

fn profile(value: &str) -> Result<ProfileMode> {
    match value {
        "off" => Ok(ProfileMode::Off),
        "phases" => Ok(ProfileMode::Phases),
        "timings" => Ok(ProfileMode::Timings),
        _ => Err(Error::invalid_argument(format!(
            "Invalid profile mode {value}"
        ))),
    }
}

fn query_options(proto: proto::ConversationalQueryOptions) -> Result<ConversationalQueryOptions> {
    let mut options = ConversationalQueryOptions::new();
    if let Some(model) = proto.model {
        options = options.model(model_options(model)?);
    }
    if let Some(credential) = proto.credential {
        options = options.credential(model_credential(credential)?);
    }
    if let Some(hint) = proto.hint {
        options = options.hint(hint);
    }
    if let Some(execute) = proto.execute {
        options = options.execute(execute);
    }
    if let Some(value) = proto.output {
        options = options.output(output(value)?);
    }
    if let Some(use_knowledge) = proto.use_knowledge {
        options = options.use_knowledge(use_knowledge);
    }
    if let Some(scan_consistency) = proto.scan_consistency {
        options = match ScanConsistency::try_from(scan_consistency) {
            Ok(ScanConsistency::RequestPlus) => options
                .scan_consistency(couchbase::options::query_options::ScanConsistency::RequestPlus),
            Ok(ScanConsistency::NotBounded) => options
                .scan_consistency(couchbase::options::query_options::ScanConsistency::NotBounded),
            Err(_) => return Err(Error::invalid_argument("Invalid scan consistency")),
        };
    }
    if let Some(consistent_with) = proto.consistent_with {
        options = options.scan_consistency(
            couchbase::options::query_options::ScanConsistency::AtPlus(consistent_with.into()),
        );
    }
    if let Some(scan_wait) = proto.scan_wait_millis {
        options = options.scan_wait(duration_from_millis(scan_wait)?);
    }
    if let Some(use_replica) = proto.use_replica {
        options = options.use_replica(if use_replica {
            ReplicaLevel::On
        } else {
            ReplicaLevel::Off
        });
    }
    if let Some(client_context_id) = proto.client_context_id {
        options = options.client_context_id(client_context_id);
    }
    if let Some(metrics) = proto.metrics {
        options = options.metrics(metrics);
    }
    if let Some(mode) = proto.profile.as_deref() {
        options = options.profile(profile(mode)?);
    }
    if let Some(timeout) = proto.timeout_millis {
        options = options.server_timeout(duration_from_millis(timeout)?);
    }
    for (key, value) in json_values(proto.raw_parameters, "Raw parameter")? {
        options = options.add_raw(key, value)?;
    }
    Ok(options)
}

fn ask_options(proto: proto::AskOptions) -> Result<ConversationalAskOptions> {
    let mut options = ConversationalAskOptions::new();
    if let Some(hint) = proto.hint {
        options = options.hint(hint);
    }
    if let Some(execute) = proto.execute {
        options = options.execute(execute);
    }
    if let Some(value) = proto.output {
        options = options.output(output(value)?);
    }
    if let Some(scan_consistency) = proto.scan_consistency {
        options = match ScanConsistency::try_from(scan_consistency) {
            Ok(ScanConsistency::RequestPlus) => options
                .scan_consistency(couchbase::options::query_options::ScanConsistency::RequestPlus),
            Ok(ScanConsistency::NotBounded) => options
                .scan_consistency(couchbase::options::query_options::ScanConsistency::NotBounded),
            Err(_) => return Err(Error::invalid_argument("Invalid scan consistency")),
        };
    }
    if let Some(consistent_with) = proto.consistent_with {
        options = options.scan_consistency(
            couchbase::options::query_options::ScanConsistency::AtPlus(consistent_with.into()),
        );
    }
    if let Some(scan_wait) = proto.scan_wait_millis {
        options = options.scan_wait(duration_from_millis(scan_wait)?);
    }
    if let Some(use_replica) = proto.use_replica {
        options = options.use_replica(if use_replica {
            ReplicaLevel::On
        } else {
            ReplicaLevel::Off
        });
    }
    if let Some(client_context_id) = proto.client_context_id {
        options = options.client_context_id(client_context_id);
    }
    if let Some(metrics) = proto.metrics {
        options = options.metrics(metrics);
    }
    if let Some(mode) = proto.profile.as_deref() {
        options = options.profile(profile(mode)?);
    }
    if let Some(timeout) = proto.timeout_millis {
        options = options.server_timeout(duration_from_millis(timeout)?);
    }
    for (key, value) in json_values(proto.raw_parameters, "Raw parameter")? {
        options = options.add_raw(key, value)?;
    }
    Ok(options)
}

fn begin_chat_options(proto: proto::BeginChatOptions) -> Result<ConversationalBeginChatOptions> {
    let mut options = ConversationalBeginChatOptions::new();
    if let Some(model) = proto.model {
        options = options.model(model_options(model)?);
    }
    if let Some(credential) = proto.credential {
        options = options.credential(model_credential(credential)?);
    }
    if let Some(timeout) = proto.inactivity_timeout_millis {
        options = options.inactivity_timeout(duration_from_millis(timeout)?);
    }
    if let Some(use_knowledge) = proto.use_knowledge {
        options = options.use_knowledge(use_knowledge);
    }
    if let Some(timeout) = proto.timeout_millis {
        options = options.server_timeout(duration_from_millis(timeout)?);
    }
    Ok(options)
}

fn attach_chat_options(proto: proto::AttachChatOptions) -> Result<ConversationalAttachChatOptions> {
    let mut options = ConversationalAttachChatOptions::new();
    if let Some(model) = proto.model {
        options = options.model(model_options(model)?);
    }
    if let Some(credential) = proto.credential {
        options = options.credential(model_credential(credential)?);
    }
    Ok(options)
}
