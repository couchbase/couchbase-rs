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

use crate::log_redaction::user_data;
use tokio::select;
use tokio::time::{sleep, Instant};
use tracing::warn;

use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslauto::{OpSASLAutoEncoder, OpsSASLAuthAuto, SASLAuthAutoOptions};
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::{run_op_future_with_deadline, StandardPendingOp};
use crate::memdx::request::{
    GetClusterConfigRequest, GetErrorMapRequest, HelloRequest, SASLAuthRequest,
    SASLListMechsRequest, SASLStepRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    BootstrapResult, GetClusterConfigResponse, GetErrorMapResponse, HelloResponse,
    SASLAuthResponse, SASLListMechsResponse, SASLStepResponse, SelectBucketResponse,
};

pub trait OpBootstrapEncoder {
    fn hello<D>(
        &self,
        dispatcher: &D,
        request: HelloRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<HelloResponse>>>
    where
        D: Dispatcher;

    fn get_error_map<D>(
        &self,
        dispatcher: &D,
        request: GetErrorMapRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<GetErrorMapResponse>>>
    where
        D: Dispatcher;

    fn select_bucket<D>(
        &self,
        dispatcher: &D,
        request: SelectBucketRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<SelectBucketResponse>>>
    where
        D: Dispatcher;

    fn get_cluster_config<D>(
        &self,
        dispatcher: &D,
        request: GetClusterConfigRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<GetClusterConfigResponse>>>
    where
        D: Dispatcher;
}

pub struct OpBootstrap {}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BootstrapOptions {
    pub hello: Option<HelloRequest>,
    pub get_error_map: Option<GetErrorMapRequest>,
    pub auth: Option<SASLAuthAutoOptions>,
    pub select_bucket: Option<SelectBucketRequest>,
    pub deadline: Instant,
    pub get_cluster_config: Option<GetClusterConfigRequest>,
}

impl OpBootstrap {
    // bootstrap is currently not pipelined. SCRAM, and the general retry behaviour within sasl auto,
    // make pipelining complex. It's a bit of a niche optimization so we can improve later.
    pub async fn bootstrap<E, D>(
        encoder: E,
        dispatcher: &D,
        opts: BootstrapOptions,
    ) -> Result<BootstrapResult>
    where
        E: OpBootstrapEncoder + OpSASLAutoEncoder,
        D: Dispatcher,
    {
        let mut result = BootstrapResult {
            hello: None,
            error_map: None,
            cluster_config: None,
        };

        if let Some(req) = opts.hello {
            result.hello =
                match run_op_future_with_deadline(opts.deadline, encoder.hello(dispatcher, req))
                    .await
                {
                    Ok(r) => Some(r),
                    Err(e) => {
                        warn!("Hello failed {}", user_data(&e));
                        None
                    }
                };
        };

        if let Some(req) = opts.get_error_map {
            result.error_map = match run_op_future_with_deadline(
                opts.deadline,
                encoder.get_error_map(dispatcher, req),
            )
            .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get error map failed {}", user_data(&e));
                    None
                }
            };
        };
        if let Some(req) = opts.auth {
            let op_auto = OpsSASLAuthAuto {};
            match op_auto
                .sasl_auth_auto(&encoder, dispatcher, opts.deadline, req)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Auth failed {}", user_data(&e));
                    return Err(e);
                }
            };
        }

        if let Some(req) = opts.select_bucket {
            match run_op_future_with_deadline(opts.deadline, encoder.select_bucket(dispatcher, req))
                .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Select bucket failed {}", user_data(&e));
                    return Err(e);
                }
            };
        }

        if let Some(req) = opts.get_cluster_config {
            result.cluster_config = match run_op_future_with_deadline(
                opts.deadline,
                encoder.get_cluster_config(dispatcher, req),
            )
            .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get cluster config failed {}", user_data(&e));
                    None
                }
            }
        };

        Ok(result)
    }
}
