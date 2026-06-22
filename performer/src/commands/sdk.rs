use crate::cluster::connection::ConnectionSet;
use crate::commands::authenticator::AuthenticatorCommand;
use crate::commands::bucket_management::BucketManagerCommand;
use crate::commands::collection_management::CollectionManagerCommand;
use crate::commands::helpers::current_timestamp;
use crate::commands::kv::{
    ExistsCommand, GetAndLockCommand, GetAndTouchCommand, GetCommand, InsertCommand, KvCommand,
    RemoveCommand, ReplaceCommand, TouchCommand, UnlockCommand, UpsertCommand,
};
use crate::commands::kv_binary::{
    AppendCommand, DecrementCommand, IncrementCommand, PrependCommand,
};
use crate::commands::query::{Location as QueryLocation, QueryCommand};
use crate::commands::query_index_management::QueryIndexManagerCommand;
use crate::commands::search::SearchCommand;
use crate::commands::search_index_management::SearchIndexManagerCommand;
use crate::commands::subdoc::{LookupInCommand, MutateInCommand};
use crate::commands::wait_until_ready::{Location, WaitUntilReadyCommand};
use crate::common::content::{content_from_shared, transcoder_from_shared, SdkContent, Transcoder};
use crate::common::location::DocLocation;
use crate::counters::counter::Counters;
use crate::errors::error::{Error, Result};
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::sdk::command::Command;
use crate::proto::protocol::sdk::kv::lookup_in::lookup_in_spec::Operation;
use crate::proto::protocol::sdk::kv::lookup_in::LookupIn;
use crate::proto::protocol::sdk::kv::mutate_in::content_or_macro::ContentOrMacro;
use crate::proto::protocol::sdk::kv::mutate_in::{mutate_in_spec, MutateIn, MutateInMacro};
use crate::proto::protocol::sdk::kv::{
    Append, Decrement, Exists, Get, GetAndLock, GetAndLockOptions, GetAndTouch, GetAndTouchOptions,
    GetOptions, Increment, Insert, InsertOptions, Prepend, Remove, Replace, ReplaceOptions, Touch,
    Unlock, Upsert, UpsertOptions,
};
use crate::proto::protocol::sdk::{
    bucket_level_command, cluster_level_command, collection_level_command, scope_level_command,
};
use crate::proto::protocol::{sdk, shared};
use crate::streams::stream_owner::StreamOwner;
use couchbase::scope::Scope;
use couchbase::subdoc::lookup_in_specs::{
    CountSpecOptions, ExistsSpecOptions, GetSpecOptions, LookupInSpec,
};
use couchbase::subdoc::macros::MutateInMacros;
use couchbase::subdoc::mutate_in_specs::{
    ArrayAddUniqueSpecOptions, ArrayAppendSpecOptions, ArrayInsertSpecOptions,
    ArrayPrependSpecOptions, DecrementSpecOptions, IncrementSpecOptions, InsertSpecOptions,
    MutateInSpec, RemoveSpecOptions, ReplaceSpecOptions, UpsertSpecOptions,
};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub(crate) const MANAGEMENT_TIMEOUT: Duration = Duration::from_secs(75);
pub(crate) const KV_TIMEOUT: Duration = Duration::from_millis(2500);
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(75);
pub(crate) const SEARCH_TIMEOUT: Duration = Duration::from_secs(75);

pub enum SdkCommand {
    KV(KvCommand),
    Query(QueryCommand),
    QueryManagement(QueryIndexManagerCommand),
    CollectionManagement(CollectionManagerCommand),
    BucketManagement(BucketManagerCommand),
    SearchIndexManagement(SearchIndexManagerCommand),
    WaitUntilReady(WaitUntilReadyCommand),
    Search(SearchCommand),
    Authenticator(AuthenticatorCommand),
    // ...
}

impl SdkCommand {
    pub async fn execute(
        self,
        batcher: &crate::common::batcher::Batcher,
        stream_owner: &Arc<StreamOwner>,
        run_id: String,
    ) -> Result<bool> {
        match self {
            SdkCommand::KV(cmd) => run_with_timeout(KV_TIMEOUT, cmd.execute(batcher)).await,
            SdkCommand::Query(cmd) => run_with_timeout(QUERY_TIMEOUT, cmd.execute(batcher)).await,
            SdkCommand::QueryManagement(cmd) => {
                run_with_timeout(MANAGEMENT_TIMEOUT, cmd.execute(batcher)).await
            }
            SdkCommand::CollectionManagement(cmd) => {
                run_with_timeout(MANAGEMENT_TIMEOUT, cmd.execute(batcher)).await
            }
            SdkCommand::BucketManagement(cmd) => {
                run_with_timeout(MANAGEMENT_TIMEOUT, cmd.execute(batcher)).await
            }
            SdkCommand::SearchIndexManagement(cmd) => {
                run_with_timeout(MANAGEMENT_TIMEOUT, cmd.execute(batcher)).await
            }
            SdkCommand::WaitUntilReady(cmd) => {
                run_with_timeout(MANAGEMENT_TIMEOUT, cmd.execute(batcher)).await
            }
            SdkCommand::Search(cmd) => {
                run_with_timeout(SEARCH_TIMEOUT, cmd.execute(batcher, stream_owner, run_id)).await
            }
            SdkCommand::Authenticator(cmd) => cmd.execute(batcher).await,
        }
    }
}

/// Helper to run a future with a timeout, returning a timeout error if elapsed.
pub async fn run_with_timeout<F, T>(duration: Duration, fut: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match timeout(duration, fut).await {
        Ok(res) => res,
        Err(_) => Err(Box::new(Error::Status(tonic::Status::deadline_exceeded(
            "operation timed out",
        )))),
    }
}

pub fn build_sdk_command(
    conn: Arc<ConnectionSet>,
    proto: sdk::Command,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
) -> Result<SdkCommand> {
    match proto.command {
        Some(Command::Get(get)) => {
            build_get_command(&conn, get, counters, span_owner, proto.return_result)
        }
        Some(Command::Insert(insert)) => {
            build_insert_command(&conn, insert, counters, span_owner, proto.return_result)
        }
        Some(Command::Replace(replace)) => {
            build_replace_command(&conn, replace, counters, span_owner, proto.return_result)
        }
        Some(Command::Upsert(upsert)) => {
            build_upsert_command(&conn, upsert, counters, span_owner, proto.return_result)
        }
        Some(Command::Remove(remove)) => {
            build_remove_command(&conn, remove, counters, span_owner, proto.return_result)
        }
        Some(Command::ClusterCommand(cluster_cmd)) => {
            build_cluster_command(&conn, cluster_cmd, span_owner, proto.return_result)
        }
        Some(Command::BucketCommand(bucket_cmd)) => {
            build_bucket_command(&conn, bucket_cmd, span_owner, proto.return_result)
        }
        Some(Command::ScopeCommand(scope_cmd)) => {
            build_scope_command(&conn, scope_cmd, span_owner, proto.return_result)
        }
        Some(Command::CollectionCommand(collection_cmd)) => build_collection_command(
            conn,
            collection_cmd,
            counters,
            span_owner,
            proto.return_result,
        ),
        _ => Err(Error::unimplemented("Unsupported command type")),
    }
}

fn build_cluster_command(
    conn: &Arc<ConnectionSet>,
    cluster_cmd: sdk::ClusterLevelCommand,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let cluster = conn.cluster.clone();

    match cluster_cmd.command.unwrap() {
        cluster_level_command::Command::BucketManager(cmd) => Ok(SdkCommand::BucketManagement(
            BucketManagerCommand::new(cluster, cmd.command.unwrap(), return_result, span_owner)?,
        )),
        cluster_level_command::Command::WaitUntilReady(cmd) => Ok(SdkCommand::WaitUntilReady(
            WaitUntilReadyCommand::new(Location::Cluster(cluster), cmd, return_result),
        )),
        cluster_level_command::Command::Query(cmd) => build_query_command(
            QueryLocation::Cluster(cluster),
            cmd,
            span_owner,
            return_result,
        ),
        cluster_level_command::Command::SearchV2(_cmd) => Err(Error::unimplemented(
            "Cluster level search command is not implemented",
        )),
        cluster_level_command::Command::QueryIndexManager(_cmd) => Err(Error::unimplemented(
            "Cluster level query index management command not implemented",
        )),
        cluster_level_command::Command::Authenticator(cmd) => Ok(SdkCommand::Authenticator(
            AuthenticatorCommand::new(cluster, cmd.authenticator.unwrap(), return_result),
        )),
        _ => Err(Error::unimplemented("Unimplemented cluster command type")),
    }
}

fn build_bucket_command(
    conn: &Arc<ConnectionSet>,
    bucket_cmd: sdk::BucketLevelCommand,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let bucket = conn.cluster.bucket(&bucket_cmd.bucket_name);
    let command = bucket_cmd
        .command
        .ok_or_else(|| Error::unimplemented("Bucket command not set"))?;

    match command {
        bucket_level_command::Command::CollectionManager(cmd) => Ok(
            SdkCommand::CollectionManagement(CollectionManagerCommand::new(
                bucket,
                cmd.command.unwrap(),
                return_result,
                span_owner,
            )?),
        ),
        bucket_level_command::Command::WaitUntilReady(cmd) => Ok(SdkCommand::WaitUntilReady(
            WaitUntilReadyCommand::new(Location::Bucket(bucket), cmd, return_result),
        )),
    }
}

fn build_scope_command(
    conn: &Arc<ConnectionSet>,
    scope_cmd: sdk::ScopeLevelCommand,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let scope = scope_cmd
        .scope
        .ok_or_else(|| Error::invalid_argument("Scope not set in ScopeLevelCommand"))
        .map(|location| {
            let bucket = conn.cluster.bucket(&location.bucket_name);
            bucket.scope(&location.scope_name)
        })?;

    match scope_cmd.command.unwrap() {
        scope_level_command::Command::Query(query) => build_query_command(
            QueryLocation::Scope(scope),
            query,
            span_owner,
            return_result,
        ),
        scope_level_command::Command::SearchV2(search) => {
            build_search_command(scope, search, span_owner)
        }
        scope_level_command::Command::SearchIndexManager(cmd) => Ok(
            SdkCommand::SearchIndexManagement(SearchIndexManagerCommand::new(
                scope,
                cmd.command.unwrap(),
                return_result,
                span_owner,
            )?),
        ),
        _ => Err(Error::unimplemented("Unimplemented scope command type")),
    }
}

fn build_collection_command(
    conn: Arc<ConnectionSet>,
    collection_cmd: sdk::CollectionLevelCommand,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let command = collection_cmd
        .command
        .ok_or_else(|| Error::unimplemented("Collection command not set"))?;

    match command {
        collection_level_command::Command::GetAndLock(get_and_lock) => {
            build_get_and_lock_command(conn, get_and_lock, counters, span_owner, return_result)
        }
        collection_level_command::Command::GetAndTouch(get_and_touch) => {
            build_get_and_touch_command(conn, get_and_touch, counters, span_owner, return_result)
        }
        collection_level_command::Command::Unlock(unlock) => {
            build_unlock_command(conn, unlock, counters, span_owner, return_result)
        }
        collection_level_command::Command::Exists(exists) => {
            build_exists_command(conn, exists, counters, span_owner, return_result)
        }
        collection_level_command::Command::Touch(touch) => {
            build_touch_command(conn, touch, counters, span_owner, return_result)
        }
        collection_level_command::Command::Binary(binary) => {
            build_binary_command(conn, binary, counters, span_owner, return_result)
        }
        collection_level_command::Command::LookupIn(lookup_in) => {
            build_lookup_in_command(conn, lookup_in, counters, span_owner, return_result)
        }
        collection_level_command::Command::MutateIn(mutate_in) => {
            build_mutate_in_command(conn, mutate_in, counters, span_owner, return_result)
        }
        collection_level_command::Command::QueryIndexManager(cmd) => {
            let doc_location = collection_cmd.collection.unwrap();
            let collection = conn
                .cluster
                .bucket(doc_location.bucket_name)
                .scope(doc_location.scope_name)
                .collection(doc_location.collection_name);

            Ok(SdkCommand::QueryManagement(QueryIndexManagerCommand::new(
                collection,
                cmd.command.unwrap(),
                return_result,
                span_owner,
            )?))
        }
        _ => Err(Error::unimplemented(
            "Unimplemented collection command type",
        )),
    }
}

fn build_binary_command(
    conn: Arc<ConnectionSet>,
    binary: sdk::BinaryCollectionLevelCommand,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let command = binary
        .command
        .ok_or_else(|| Error::unimplemented("Collection command not set"))?;

    match command {
        sdk::binary_collection_level_command::Command::Append(append) => {
            build_append_command(conn, append, counters, span_owner, return_result)
        }
        sdk::binary_collection_level_command::Command::Prepend(prepend) => {
            build_prepend_command(conn, prepend, counters, span_owner, return_result)
        }
        sdk::binary_collection_level_command::Command::Increment(increment) => {
            build_increment_command(conn, increment, counters, span_owner, return_result)
        }
        sdk::binary_collection_level_command::Command::Decrement(decrement) => {
            build_decrement_command(conn, decrement, counters, span_owner, return_result)
        }
    }
}

fn build_mutate_in_command(
    conn: Arc<ConnectionSet>,
    mutate_in: MutateIn,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&mutate_in.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let mut contents_as = Vec::with_capacity(mutate_in.spec.len());
    let mut specs = Vec::with_capacity(mutate_in.spec.len());
    for spec in mutate_in.spec.into_iter() {
        let content_as = spec.content_as.map(|c| c.r#as.unwrap());

        contents_as.push(content_as);

        let couchbase_spec = match spec.operation.unwrap() {
            mutate_in_spec::Operation::Upsert(op) => {
                let mut opts = UpsertSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                match op.content.unwrap().content_or_macro.unwrap() {
                    ContentOrMacro::Content(c) => {
                        MutateInSpec::upsert(op.path, parse_mutate_in_content(c), opts)
                    }
                    ContentOrMacro::Macro(m) => {
                        MutateInSpec::upsert(op.path, parse_mutate_in_macros(m)?, opts)
                    }
                }
            }
            mutate_in_spec::Operation::Insert(op) => {
                let mut opts = InsertSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                match op.content.unwrap().content_or_macro.unwrap() {
                    ContentOrMacro::Content(c) => {
                        MutateInSpec::insert(op.path, parse_mutate_in_content(c), opts)
                    }
                    ContentOrMacro::Macro(m) => {
                        MutateInSpec::insert(op.path, parse_mutate_in_macros(m)?, opts)
                    }
                }
            }
            mutate_in_spec::Operation::Replace(op) => {
                let mut opts = ReplaceSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                match op.content.unwrap().content_or_macro.unwrap() {
                    ContentOrMacro::Content(c) => {
                        MutateInSpec::replace(op.path, parse_mutate_in_content(c), opts)
                    }
                    ContentOrMacro::Macro(m) => {
                        MutateInSpec::replace(op.path, parse_mutate_in_macros(m)?, opts)
                    }
                }
            }
            mutate_in_spec::Operation::Remove(op) => {
                let mut opts = RemoveSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                Ok(MutateInSpec::remove(op.path, opts))
            }
            mutate_in_spec::Operation::ArrayAppend(op) => {
                let mut opts = ArrayAppendSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                let mut contents = vec![];
                for content in op.content {
                    match content.content_or_macro.unwrap() {
                        ContentOrMacro::Content(c) => {
                            contents.push(parse_mutate_in_content(c));
                        }
                        ContentOrMacro::Macro(m) => {
                            contents
                                .push(serde_json::to_value(parse_mutate_in_macros(m)?).unwrap());
                        }
                    }
                }

                MutateInSpec::array_append(op.path, &contents, opts)
            }
            mutate_in_spec::Operation::ArrayPrepend(op) => {
                let mut opts = ArrayPrependSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                let mut contents = vec![];
                for content in op.content {
                    match content.content_or_macro.unwrap() {
                        ContentOrMacro::Content(c) => {
                            contents.push(parse_mutate_in_content(c));
                        }
                        ContentOrMacro::Macro(m) => {
                            contents
                                .push(serde_json::to_value(parse_mutate_in_macros(m)?).unwrap());
                        }
                    }
                }

                MutateInSpec::array_prepend(op.path, &contents, opts)
            }
            mutate_in_spec::Operation::ArrayInsert(op) => {
                let mut opts = ArrayInsertSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                let mut contents = vec![];
                for content in op.content {
                    match content.content_or_macro.unwrap() {
                        ContentOrMacro::Content(c) => {
                            contents.push(parse_mutate_in_content(c));
                        }
                        ContentOrMacro::Macro(m) => {
                            contents
                                .push(serde_json::to_value(parse_mutate_in_macros(m)?).unwrap());
                        }
                    }
                }

                MutateInSpec::array_insert(op.path, &contents, opts)
            }
            mutate_in_spec::Operation::ArrayAddUnique(op) => {
                let mut opts = ArrayAddUniqueSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                match op.content.unwrap().content_or_macro.unwrap() {
                    ContentOrMacro::Content(c) => {
                        MutateInSpec::array_add_unique(op.path, parse_mutate_in_content(c), opts)
                    }
                    ContentOrMacro::Macro(m) => {
                        MutateInSpec::array_add_unique(op.path, parse_mutate_in_macros(m)?, opts)
                    }
                }
            }
            mutate_in_spec::Operation::Increment(op) => {
                let mut opts = IncrementSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                MutateInSpec::increment(op.path, op.delta, opts)
            }
            mutate_in_spec::Operation::Decrement(op) => {
                let mut opts = DecrementSpecOptions::new();

                if let Some(xattr) = op.xattr {
                    opts = opts.xattr(xattr);
                }

                if let Some(create_path) = op.create_path {
                    opts = opts.create_path(create_path);
                }

                MutateInSpec::decrement(op.path, op.delta, opts)
            }
        }
        // This error is only ever from serializing, so should be fine to unwrap.
        // Trying to propagate this error would also be more effort than it's worth.
        .unwrap();

        specs.push(couchbase_spec);
    }

    let parent_span = mutate_in
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    let opts = mutate_in.options.map(|opts| opts.try_into()).transpose()?;

    Ok(SdkCommand::KV(KvCommand::MutateIn(MutateInCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        specs,
        contents_as,
        opts,
        parent_span,
    ))))
}

fn build_lookup_in_command(
    conn: Arc<ConnectionSet>,
    lookup_in: LookupIn,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&lookup_in.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let mut specs = Vec::with_capacity(lookup_in.spec.len());
    let mut contents_as = Vec::with_capacity(lookup_in.spec.len());
    for spec in lookup_in.spec.into_iter() {
        if let Some(content_as) = spec.content_as {
            if let Some(content_as) = content_as.r#as {
                contents_as.push(content_as);
            }
        }

        let couchbase_spec = match spec.operation.unwrap() {
            Operation::Exists(s) => {
                let mut opts = ExistsSpecOptions::new();

                if let Some(xattr) = s.xattr {
                    opts = opts.xattr(xattr);
                }
                LookupInSpec::exists(s.path, opts)
            }
            Operation::Get(s) => {
                let mut opts = GetSpecOptions::new();
                if let Some(xattr) = s.xattr {
                    opts = opts.xattr(xattr);
                }

                LookupInSpec::get(s.path, opts)
            }
            Operation::Count(s) => {
                let mut opts = CountSpecOptions::new();
                if let Some(xattr) = s.xattr {
                    opts = opts.xattr(xattr);
                }

                LookupInSpec::count(s.path, opts)
            }
        };

        specs.push(couchbase_spec);
    }

    let parent_span = lookup_in
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    let opts = lookup_in.options.map(|opts| opts.try_into()).transpose()?;

    Ok(SdkCommand::KV(KvCommand::LookupIn(LookupInCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        specs,
        contents_as,
        opts,
        parent_span,
    ))))
}

fn build_query_command(
    location: QueryLocation,
    query: sdk::query::Command,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let content_as = query
        .content_as
        .map(|c| {
            c.r#as
                .ok_or_else(|| Error::invalid_argument("Query command must have a content_as"))
        })
        .transpose()?;

    let parent_span = query
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_deref())
        .and_then(|id| span_owner.get(id));

    let opts = query.options.map(|opts| opts.try_into()).transpose()?;

    let command = QueryCommand::new(
        location,
        query.statement.clone(),
        return_result,
        current_timestamp(),
        content_as,
        opts,
        parent_span,
    );

    Ok(SdkCommand::Query(command))
}

fn build_search_command(
    scope: Scope,
    search: sdk::search::SearchWrapper,
    span_owner: Arc<SpanOwner>,
) -> Result<SdkCommand> {
    let search_wrapper = search.search.unwrap();

    let request = search_wrapper.request.unwrap().try_into()?;
    let fields_as = search.fields_as.and_then(|c| c.r#as);

    let parent_span = search_wrapper
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_deref())
        .and_then(|id| span_owner.get(id));

    let opts = search_wrapper
        .options
        .map(|opts| opts.try_into())
        .transpose()?;

    let command = SearchCommand::new(
        scope,
        search_wrapper.index_name,
        request,
        current_timestamp(),
        opts,
        fields_as,
        search.stream_config.unwrap(),
        parent_span,
    );

    Ok(SdkCommand::Search(command))
}

fn build_get_command(
    conn: &Arc<ConnectionSet>,
    get: Get,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&get.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());
    let transcoder = parse_transcoder(&get.options)?;

    let content_as = get.content_as.and_then(|ca| ca.r#as);

    let parent_span = get
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Get(GetCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        content_as,
        transcoder,
        get.options
            .clone()
            .map(|opts| opts.try_into())
            .transpose()?,
        parent_span,
    ))))
}

fn build_insert_command(
    conn: &Arc<ConnectionSet>,
    insert: Insert,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&insert.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());
    let content = parse_content(insert.content.unwrap())?;
    let transcoder = parse_transcoder(&insert.options)?;

    let parent_span = insert
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Insert(InsertCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        content,
        transcoder,
        insert.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_replace_command(
    conn: &Arc<ConnectionSet>,
    replace: Replace,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&replace.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());
    let content = parse_content(replace.content.unwrap())?;
    let transcoder = parse_transcoder(&replace.options)?;

    let parent_span = replace
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Replace(ReplaceCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        content,
        transcoder,
        replace.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_upsert_command(
    conn: &Arc<ConnectionSet>,
    upsert: Upsert,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&upsert.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());
    let content = parse_content(upsert.content.unwrap())?;
    let transcoder = parse_transcoder(&upsert.options)?;

    let parent_span = upsert
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Upsert(UpsertCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        content,
        transcoder,
        upsert.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_remove_command(
    conn: &Arc<ConnectionSet>,
    remove: Remove,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&remove.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let parent_span = remove
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Remove(RemoveCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        remove.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_get_and_lock_command(
    conn: Arc<ConnectionSet>,
    get_and_lock: GetAndLock,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&get_and_lock.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let transcoder = parse_transcoder(&get_and_lock.options)?;

    let content_as = get_and_lock.content_as.and_then(|ca| ca.r#as);

    let duration = get_and_lock
        .duration
        .ok_or_else(|| Error::internal("Duration missing in get_and_lock"))?
        .try_into()
        .map_err(|e| Error::internal(format!("Duration conversion failed: {e}")))?;

    let parent_span = get_and_lock
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::GetAndLock(
        GetAndLockCommand::new(
            collection,
            doc_location.id().to_string(),
            duration,
            return_result,
            current_timestamp(),
            content_as,
            transcoder,
            get_and_lock
                .options
                .clone()
                .map(|opts| opts.try_into())
                .transpose()?,
            parent_span,
        ),
    )))
}

fn build_get_and_touch_command(
    conn: Arc<ConnectionSet>,
    get_and_touch: GetAndTouch,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&get_and_touch.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let transcoder = parse_transcoder(&get_and_touch.options)?;

    let content_as = get_and_touch.content_as.and_then(|ca| ca.r#as);

    let expiry = get_and_touch
        .expiry
        .ok_or_else(|| Error::invalid_argument("Get and lock command must have a expiry"))?
        .try_into()
        .map_err(|e| Error::internal(format!("Expiry conversion failed: {e}")))?;

    let parent_span = get_and_touch
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::GetAndTouch(
        GetAndTouchCommand::new(
            collection,
            doc_location.id().to_string(),
            expiry,
            return_result,
            current_timestamp(),
            content_as,
            transcoder,
            get_and_touch
                .options
                .clone()
                .map(|opts| opts.try_into())
                .transpose()?,
            parent_span,
        ),
    )))
}

fn build_unlock_command(
    conn: Arc<ConnectionSet>,
    unlock: Unlock,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&unlock.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let parent_span = unlock
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Unlock(UnlockCommand::new(
        collection,
        doc_location.id().to_string(),
        unlock.cas as u64,
        return_result,
        current_timestamp(),
        unlock
            .options
            .clone()
            .map(|opts| opts.try_into())
            .transpose()?,
        parent_span,
    ))))
}

fn build_exists_command(
    conn: Arc<ConnectionSet>,
    exists: Exists,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&exists.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let parent_span = exists
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Exists(ExistsCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        exists
            .options
            .clone()
            .map(|opts| opts.try_into())
            .transpose()?,
        parent_span,
    ))))
}

fn build_touch_command(
    conn: Arc<ConnectionSet>,
    touch: Touch,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&touch.location, counters)?;
    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name());

    let expiry = touch
        .expiry
        .ok_or_else(|| Error::invalid_argument("Get and lock command must have a expiry"))?
        .try_into()
        .map_err(|e| Error::internal(format!("Expiry conversion failed: {e}")))?;

    let parent_span = touch
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Touch(TouchCommand::new(
        collection,
        doc_location.id().to_string(),
        expiry,
        return_result,
        current_timestamp(),
        touch
            .options
            .clone()
            .map(|opts| opts.try_into())
            .transpose()?,
        parent_span,
    ))))
}

fn build_append_command(
    conn: Arc<ConnectionSet>,
    append: Append,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&append.location, counters)?;

    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name())
        .binary();

    let parent_span = append
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Append(AppendCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        append.content.clone(),
        append.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_prepend_command(
    conn: Arc<ConnectionSet>,
    prepend: Prepend,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&prepend.location, counters)?;

    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name())
        .binary();

    let parent_span = prepend
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Prepend(PrependCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        prepend.content.clone(),
        prepend.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_increment_command(
    conn: Arc<ConnectionSet>,
    increment: Increment,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&increment.location, counters)?;

    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name())
        .binary();

    let parent_span = increment
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Increment(IncrementCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        increment.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn build_decrement_command(
    conn: Arc<ConnectionSet>,
    decrement: Decrement,
    counters: Arc<Counters>,
    span_owner: Arc<SpanOwner>,
    return_result: bool,
) -> Result<SdkCommand> {
    let doc_location = parse_doc_location(&decrement.location, counters)?;

    let collection = conn
        .cluster
        .bucket(doc_location.bucket())
        .scope(doc_location.scope())
        .collection(doc_location.collection_name())
        .binary();

    let parent_span = decrement
        .options
        .as_ref()
        .and_then(|o| o.parent_span_id.as_ref())
        .and_then(|id| span_owner.get(id));

    Ok(SdkCommand::KV(KvCommand::Decrement(DecrementCommand::new(
        collection,
        doc_location.id().to_string(),
        return_result,
        current_timestamp(),
        decrement.options.map(|opts| opts.try_into()).transpose()?,
        parent_span,
    ))))
}

fn parse_mutate_in_macros(m: i32) -> Result<MutateInMacros> {
    match MutateInMacro::try_from(m) {
        Ok(MutateInMacro::Cas) => Ok(MutateInMacros::CAS),
        Ok(MutateInMacro::SeqNo) => Ok(MutateInMacros::SeqNo),
        Ok(MutateInMacro::ValueCrc32c) => Ok(MutateInMacros::ValueCrc32c),
        _ => Err(Error::unimplemented("Unsupported mutate in macro")),
    }
}

fn parse_mutate_in_content(content: shared::Content) -> Value {
    let content = content.content.unwrap();

    match content {
        shared::content::Content::PassthroughString(s) => Value::String(s),
        shared::content::Content::ConvertToJson(j) => serde_json::from_slice(&j).unwrap(),
        shared::content::Content::ByteArray(ba) => serde_json::to_value(&ba).unwrap(),
        shared::content::Content::Null(_) => Value::Null,
    }
}

fn parse_content(content: shared::Content) -> Result<SdkContent> {
    let content = content.content.unwrap();

    content_from_shared(content)
}

fn parse_doc_location(
    location: &Option<shared::DocLocation>,
    counters: Arc<Counters>,
) -> Result<DocLocation> {
    let location = location
        .as_ref()
        .ok_or_else(|| Error::invalid_argument("Get command must have a location"))?;
    DocLocation::from_proto(location, counters)
}

trait HasTranscoder {
    fn transcoder(&self) -> &Option<shared::Transcoder>;
}

impl HasTranscoder for GetOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

impl HasTranscoder for GetAndLockOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

impl HasTranscoder for GetAndTouchOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

impl HasTranscoder for InsertOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

impl HasTranscoder for ReplaceOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

impl HasTranscoder for UpsertOptions {
    fn transcoder(&self) -> &Option<shared::Transcoder> {
        &self.transcoder
    }
}

fn parse_transcoder(options: &Option<impl HasTranscoder>) -> Result<Option<Transcoder>> {
    options
        .as_ref()
        .and_then(|opts| opts.transcoder().as_ref())
        .and_then(|t| t.transcoder.as_ref())
        .map(transcoder_from_shared)
        .transpose()
}
