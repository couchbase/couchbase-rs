use crate::cluster::connection::ConnectionSet;
use crate::common::batcher::Batcher;
use crate::common::executor::Executor;
use crate::common::horizontal_scale_runner::{HorizontalScaleRunner, PerHorizontalRunner};
use crate::common::metrics::MetricsReporter;
use crate::counters::counter::Counters;
use crate::observability::span_owner::SpanOwner;
use crate::proto::protocol::observability::{
    SpanCreateRequest, SpanCreateResponse, SpanFinishRequest, SpanFinishResponse,
};
use crate::proto::protocol::performer::{
    Caps, PerformerCapsFetchRequest, PerformerCapsFetchResponse,
};
use crate::proto::protocol::performer_service_server::PerformerService;
use crate::proto::protocol::shared::{
    Api, ClearAllCountersRequest, ClearAllCountersResponse, ClusterConnectionCloseRequest,
    ClusterConnectionCloseResponse, ClusterConnectionCreateRequest,
    ClusterConnectionCreateResponse, Counter, DisconnectConnectionsRequest,
    DisconnectConnectionsResponse, EchoRequest, EchoResponse, SetCounterResponse,
};
use crate::proto::protocol::streams::{
    CancelRequest, CancelResponse, RequestItemsRequest, RequestItemsResponse,
};
use crate::proto::protocol::transactions::{
    CleanupSetFetchRequest, CleanupSetFetchResponse, ClientRecordProcessRequest,
    ClientRecordProcessResponse, ClientRecordRemoveRequest, ClientRecordRemoveResponse,
    CloseTransactionsRequest, CloseTransactionsResponse, CreateConnectionRequest,
    CreateConnectionResponse, TransactionCleanupAtrRequest, TransactionCleanupAtrResult,
    TransactionCleanupAttempt, TransactionCleanupQueueResult, TransactionCleanupRequest,
    TransactionCreateRequest, TransactionGenericResponse, TransactionProcessCleanupQueueRequest,
    TransactionResult, TransactionSingleQueryRequest, TransactionSingleQueryResponse,
    TransactionStreamDriverToPerformer, TransactionsFactoryCloseRequest,
    TransactionsFactoryCreateRequest, TransactionsFactoryCreateResponse,
};
use crate::proto::protocol::{run, sdk, transactions};
use crate::streams::stream_owner::StreamOwner;
use log::{error, info, trace};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{async_trait, Code, Request, Response, Status, Streaming};
use tracing::instrument::WithSubscriber;
use uuid::Uuid;

#[derive(Default)]
pub struct Performer {
    state: Mutex<PerformerState>,
    stream_owner: Arc<StreamOwner>,
    span_owner: Arc<SpanOwner>,
    counters: Arc<Counters>,
}

#[derive(Default)]
struct PerformerState {
    connections: HashMap<String, Arc<ConnectionSet>>,
}

#[async_trait]
impl PerformerService for Performer {
    async fn performer_caps_fetch(
        &self,
        _request: Request<PerformerCapsFetchRequest>,
    ) -> Result<Response<PerformerCapsFetchResponse>, Status> {
        info!("Performer caps fetch called");

        let sdk_implementation_caps = vec![
            sdk::Caps::SdkPreserveExpiry,
            sdk::Caps::SdkCollectionQueryIndexManagement,
            sdk::Caps::SdkSearch,
            sdk::Caps::SdkVectorSearch,
            sdk::Caps::SdkVectorSearchBase64,
            sdk::Caps::SdkScopeSearch,
            sdk::Caps::SdkSearchRfcRevision11,
            sdk::Caps::SdkScopeSearchIndexManagement,
            sdk::Caps::WaitUntilReady,
            sdk::Caps::SdkBucketManagement,
            sdk::Caps::SdkQuery,
            sdk::Caps::SdkQueryReadFromReplica,
            sdk::Caps::SdkLookupIn,
            sdk::Caps::SdkKv,
            sdk::Caps::SdkManagementHistoryRetention,
            sdk::Caps::SdkCollectionManagement,
            sdk::Caps::SdkDocumentNotLocked,
            sdk::Caps::SdkIndexManagementRfcRevision25,
            sdk::Caps::SdkBucketSettingsNumVbuckets,
            sdk::Caps::SdkPrefilterVectorSearch,
            sdk::Caps::SdkQueryBothPositionalAndNamedParameters,
            sdk::Caps::SupportsAuthenticator,
            sdk::Caps::SdkSetAuthenticator,
            sdk::Caps::SdkJwt,
            sdk::Caps::SdkObservabilityRfcRev24,
            sdk::Caps::SdkObservabilityClusterLabels,
            sdk::Caps::SdkStableOtelSemanticConventions,
            sdk::Caps::SdkStableOtelSemanticConventionsEmittedByDefault,
        ]
        .into_iter()
        .map(|c| c.into())
        .collect();

        let performer_caps = vec![Caps::KvSupport1, Caps::Observability1]
            .into_iter()
            .map(|c| c.into())
            .collect();

        Ok(Response::new(PerformerCapsFetchResponse {
            transaction_implementations_caps: vec![],
            sdk_implementation_caps,
            performer_caps,
            performer_user_agent: "rust".to_string(),
            library_version: "0.0.1".to_string(),
            transactions_protocol_version: None,
            supported_apis: vec![Api::Default.into()],
        }))
    }

    async fn cluster_connection_create(
        &self,
        request: Request<ClusterConnectionCreateRequest>,
    ) -> Result<Response<ClusterConnectionCreateResponse>, Status> {
        let req_ref = request.get_ref();
        info!(
            "Cluster connection create called with id: {}",
            req_ref.cluster_connection_id
        );

        let auth = crate::translations::cluster_options::build_authenticator(req_ref)
            .map_err(|e| e.status())?;
        let opts = crate::translations::cluster_options::cluster_options(
            auth,
            req_ref.cluster_config.as_ref(),
        );

        let tracing_subscriber = crate::translations::observability::tracing_subscriber(
            req_ref
                .cluster_config
                .as_ref()
                .and_then(|c| c.observability_config.as_ref()),
        );

        info!(
            "Creating cluster connection with hostname: {}",
            req_ref.cluster_hostname.clone()
        );

        let connection =
            ConnectionSet::new(req_ref.cluster_hostname.clone(), opts, tracing_subscriber)
                .await
                .map_err(|e| {
                    Status::new(
                        Code::Aborted,
                        format!("Failed to create cluster connection: {e}"),
                    )
                })?;

        let cluster_connection_count = {
            let mut state = self.state.lock().unwrap();
            state
                .connections
                .insert(req_ref.cluster_connection_id.clone(), Arc::new(connection));

            state.connections.len() as i32
        };

        Ok(Response::new(ClusterConnectionCreateResponse {
            cluster_connection_count,
        }))
    }

    async fn cluster_connection_close(
        &self,
        request: Request<ClusterConnectionCloseRequest>,
    ) -> Result<Response<ClusterConnectionCloseResponse>, Status> {
        let req_ref = request.get_ref();
        info!(
            "Cluster connection close called with id: {}",
            req_ref.cluster_connection_id
        );

        let (_removed, cluster_connection_count) = {
            let mut state = self.state.lock().unwrap();

            if !state
                .connections
                .contains_key(&req_ref.cluster_connection_id)
            {
                return Err(Status::cancelled(format!(
                    "No connection with ID={}",
                    req_ref.cluster_connection_id
                )));
            }

            let removed = state.connections.remove(&req_ref.cluster_connection_id);

            (removed, state.connections.len() as i32)
        };
        // _removed (Arc<ConnectionSet>) is dropped here, outside the lock

        Ok(Response::new(ClusterConnectionCloseResponse {
            cluster_connection_count,
        }))
    }

    async fn disconnect_connections(
        &self,
        _request: Request<DisconnectConnectionsRequest>,
    ) -> Result<Response<DisconnectConnectionsResponse>, Status> {
        info!("Disconnect connections called");

        let _old_connections = {
            let mut state = self.state.lock().unwrap();
            std::mem::take(&mut state.connections)
        };
        // _old_connections dropped here, outside the lock

        Ok(Response::new(DisconnectConnectionsResponse {}))
    }

    async fn transaction_create(
        &self,
        _request: Request<TransactionCreateRequest>,
    ) -> Result<Response<TransactionResult>, Status> {
        info!("Transaction create called");

        Err(Status::unimplemented(
            "transactions_create is unimplemented",
        ))
    }

    async fn close_transactions(
        &self,
        _request: Request<CloseTransactionsRequest>,
    ) -> Result<Response<CloseTransactionsResponse>, Status> {
        info!("Close transactions called");

        Err(Status::unimplemented("close_transactions is unimplemented"))
    }

    type transactionStreamStream = FakeTxnStream; // placeholder type, replace with actual stream type

    async fn transaction_stream(
        &self,
        _request: Request<Streaming<TransactionStreamDriverToPerformer>>,
    ) -> Result<Response<Self::transactionStreamStream>, Status> {
        info!("Transaction stream called");

        Err(Status::unimplemented("transaction_stream is unimplemented"))
    }

    async fn transaction_cleanup(
        &self,
        _request: Request<TransactionCleanupRequest>,
    ) -> Result<Response<TransactionCleanupAttempt>, Status> {
        info!("Transaction cleanup called");

        Err(Status::unimplemented(
            "transaction_cleanup is unimplemented",
        ))
    }

    async fn transaction_cleanup_atr(
        &self,
        _request: Request<TransactionCleanupAtrRequest>,
    ) -> Result<Response<TransactionCleanupAtrResult>, Status> {
        info!("Transaction cleanup ATR called");

        Err(Status::unimplemented(
            "transaction_cleanup_atr is unimplemented",
        ))
    }

    async fn cleanup_set_fetch(
        &self,
        _request: Request<CleanupSetFetchRequest>,
    ) -> Result<Response<CleanupSetFetchResponse>, Status> {
        info!("Cleanup set fetch called");

        Err(Status::unimplemented("cleanup_set_fetch is unimplemented"))
    }

    async fn client_record_process(
        &self,
        _request: Request<ClientRecordProcessRequest>,
    ) -> Result<Response<ClientRecordProcessResponse>, Status> {
        info!("Client record process called");

        Err(Status::unimplemented(
            "client_record_process is unimplemented",
        ))
    }

    async fn transaction_single_query(
        &self,
        _request: Request<TransactionSingleQueryRequest>,
    ) -> Result<Response<TransactionSingleQueryResponse>, Status> {
        info!("Transaction single query called");

        Err(Status::unimplemented(
            "transaction_single_query is unimplemented",
        ))
    }

    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let req_ref = request.get_ref();
        info!(
            "========== {} : ========== {}",
            req_ref.test_name, req_ref.message
        );

        Ok(Response::new(EchoResponse {}))
    }

    type runStream = UnboundedReceiverStream<Result<run::Result, Status>>;
    async fn run(
        &self,
        request: Request<run::Request>,
    ) -> Result<Response<Self::runStream>, Status> {
        info!("Run called");
        trace!("Run request: {request:#?}");

        let req_ref = request.get_ref();

        let workloads = match &req_ref.request {
            Some(run::request::Request::Workloads(workloads)) => workloads.clone(),
            None => return Err(Status::aborted("No request passed onto run request")),
        };

        let conn_set = {
            let state = self.state.lock().unwrap();
            state
                .connections
                .get(&workloads.cluster_connection_id)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "No connection with ID={}",
                        &workloads.cluster_connection_id
                    ))
                })?
                .clone()
        };

        let subscriber = conn_set.tracing_subscriber.clone();

        let config = req_ref.config.as_ref();

        let counters = self.counters.clone();
        let run_id = Uuid::new_v4().to_string();
        let executor = Arc::new(Executor::new(
            conn_set,
            counters.clone(),
            self.stream_owner.clone(),
            self.span_owner.clone(),
            run_id.clone(),
        ));

        let mut batch_size = 0;

        let (tx, rx) = mpsc::unbounded_channel();

        let shutdown_token = CancellationToken::new();
        if let Some(cfg) = config.and_then(|c| c.streaming_config.as_ref()) {
            if cfg.enable_metrics {
                let metrics_reporter = MetricsReporter::new(run_id.clone(), tx.clone());
                let metrics_shutdown = shutdown_token.clone();

                tokio::spawn(async move {
                    metrics_reporter.run_reporter(metrics_shutdown).await;
                });
            }
            if let Some(size) = cfg.batch_size {
                batch_size = size;
            }
        }

        let batcher = Arc::new(Batcher::new(batch_size));
        batcher.clone().run(tx.clone());

        // Clone the stream_owner so we can move it into the spawned task
        let stream_owner = self.stream_owner.clone();

        // We spawn a task here since the ReceiverStream needs to be returned immediately
        tokio::spawn(async move {
            let mut scale_runners = Vec::with_capacity(workloads.horizontal_scaling.len());

            for scale in workloads.horizontal_scaling {
                let mut runner = HorizontalScaleRunner::new(
                    executor.clone(),
                    PerHorizontalRunner::new(
                        batcher.clone(),
                        counters.clone(),
                        scale.workloads.clone(),
                    ),
                );
                let subscriber = subscriber.clone();
                scale_runners.push(tokio::spawn(
                    async move {
                        match runner.run().await {
                            Ok(_) => Ok(()),
                            Err(e) => {
                                error!("Runner failed: {e}");
                                Err(e)
                            }
                        }
                    }
                    .with_subscriber(subscriber),
                ));
            }

            async fn on_complete(
                run_id: String,
                stream_owner: Arc<StreamOwner>,
                batcher: Arc<Batcher>,
                shutdown_token: CancellationToken,
            ) {
                stream_owner.wait_for_completion(run_id).await;
                batcher.stop().await;
                shutdown_token.cancel();
            }

            info!("Started {} runners", scale_runners.len());
            for runner in scale_runners {
                match runner.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e.status()));
                        on_complete(run_id, stream_owner, batcher, shutdown_token).await;
                        return;
                    }
                    Err(e) => {
                        let status =
                            Status::internal(format!("Horizontal scale runner panicked: {e}"));
                        let _ = tx.send(Err(status));
                        on_complete(run_id, stream_owner, batcher, shutdown_token).await;
                        return;
                    }
                }
            }

            on_complete(run_id.clone(), stream_owner, batcher, shutdown_token).await;

            let _ = tx.send(Err(Status::ok(run_id)));
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn stream_cancel(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Stream cancel called for {}", req_ref.stream_id);
        self.stream_owner.remove(&req_ref.stream_id).await;
        Ok(Response::new(CancelResponse {}))
    }

    async fn stream_request_items(
        &self,
        request: Request<RequestItemsRequest>,
    ) -> Result<Response<RequestItemsResponse>, Status> {
        let req_ref = request.get_ref();

        info!("Stream request items called for {}", req_ref.stream_id);
        let mut stream = self
            .stream_owner
            .get(&req_ref.stream_id)
            .await
            .ok_or_else(|| {
                Status::unknown(format!("Stream with id {} not found", req_ref.stream_id))
            })?;
        if let Err(e) = stream.request_items(req_ref.num_items).await {
            self.stream_owner.remove(&req_ref.stream_id).await;
            return Err(e.status());
        }

        Ok(Response::new(RequestItemsResponse {}))
    }

    async fn set_counter(
        &self,
        request: Request<Counter>,
    ) -> Result<Response<SetCounterResponse>, Status> {
        let req_ref = request.get_ref();
        info!("set_counter called with id {}", req_ref.counter_id);

        self.counters.set(req_ref).map_err(|e| e.status())?;

        Ok(Response::new(SetCounterResponse {}))
    }

    async fn clear_all_counters(
        &self,
        _request: Request<ClearAllCountersRequest>,
    ) -> Result<Response<ClearAllCountersResponse>, Status> {
        info!("clear_all_counters called");

        self.counters.clear();

        Ok(Response::new(ClearAllCountersResponse {}))
    }

    async fn span_create(
        &self,
        request: Request<SpanCreateRequest>,
    ) -> Result<Response<SpanCreateResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Span create called for span id: {}", req_ref.id);

        let subscriber = {
            let state = self.state.lock().unwrap();
            let conn = state
                .connections
                .get(&req_ref.cluster_connection_id)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "No connection with ID={}",
                        req_ref.cluster_connection_id
                    ))
                })?;
            conn.tracing_subscriber.clone()
        };

        tracing::dispatcher::with_default(&subscriber, || {
            self.span_owner.create_span(req_ref, subscriber.clone())
        })?;

        Ok(Response::new(SpanCreateResponse {}))
    }

    async fn span_finish(
        &self,
        request: Request<SpanFinishRequest>,
    ) -> Result<Response<SpanFinishResponse>, Status> {
        let req_ref = request.get_ref();
        info!("Span finish called for span id: {}", req_ref.id);

        self.span_owner.finish_span(req_ref)?;

        Ok(Response::new(SpanFinishResponse {}))
    }

    async fn transactions_factory_create(
        &self,
        _request: Request<TransactionsFactoryCreateRequest>,
    ) -> Result<Response<TransactionsFactoryCreateResponse>, Status> {
        info!("Transactions factory create called");

        Err(Status::unimplemented(
            "transactions_factory_create is unimplemented",
        ))
    }

    async fn transactions_factory_close(
        &self,
        _request: Request<TransactionsFactoryCloseRequest>,
    ) -> Result<Response<TransactionGenericResponse>, Status> {
        info!("Transactions factory close called");

        Err(Status::unimplemented(
            "transactions_factory_close is unimplemented",
        ))
    }

    async fn create_connection(
        &self,
        _request: Request<CreateConnectionRequest>,
    ) -> Result<Response<CreateConnectionResponse>, Status> {
        info!("Create connection called");

        Err(Status::unimplemented("create_connection is unimplemented"))
    }

    async fn transaction_process_cleanup_queue(
        &self,
        _request: Request<TransactionProcessCleanupQueueRequest>,
    ) -> Result<Response<TransactionCleanupQueueResult>, Status> {
        info!("Transaction process cleanup queue called");

        Err(Status::unimplemented(
            "transaction_process_cleanup_queue is unimplemented",
        ))
    }

    async fn client_record_remove(
        &self,
        _request: Request<ClientRecordRemoveRequest>,
    ) -> Result<Response<ClientRecordRemoveResponse>, Status> {
        info!("Client record remove called");

        Err(Status::unimplemented(
            "client_record_remove is unimplemented",
        ))
    }
}

pub struct FakeTxnStream {}

impl tokio_stream::Stream for FakeTxnStream {
    type Item = Result<transactions::TransactionStreamPerformerToDriver, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
