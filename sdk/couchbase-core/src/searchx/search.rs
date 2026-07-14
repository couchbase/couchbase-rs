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

use crate::httpx::client::Client;
use crate::httpx::request::{Auth, OnBehalfOfInfo, Request};
use crate::httpx::response::Response;
use crate::mgmtx::mgmt::parse_response_json;
use crate::searchx::document_analysis::DocumentAnalysis;
use crate::searchx::error;
use crate::searchx::error::{Error, ServerError};
use crate::searchx::index::Index;
use crate::searchx::index_json::{SearchIndexResponseJson, SearchIndexesResponseJson};
use crate::searchx::mgmt_options::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetAllIndexesOptions, GetIndexOptions, GetIndexedDocumentsCountOptions,
    PauseIngestOptions, PingOptions, RefreshConfigOptions, ResumeIngestOptions,
    UnfreezePlanOptions, UpsertIndexOptions,
};
use crate::searchx::query_options::QueryOptions;
use crate::searchx::search_json::{DocumentAnalysisJson, IndexedDocumentsJson};
use crate::searchx::search_respreader::SearchRespReader;
use crate::tracingcomponent::{
    BeginDispatchFields, EndDispatchFields, TracingComponent, SERVICE_VALUE_SEARCH,
    SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use crate::util::get_host_port_tuple_from_uri;
use bytes::Bytes;
use http::{Method, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{instrument, Level, Span};

#[derive(Debug)]
pub struct Search<C: Client> {
    pub http_client: Arc<C>,
    pub user_agent: String,
    pub endpoint: String,
    pub canonical_endpoint: String,
    pub auth: Auth,

    pub vector_search_enabled: bool,
    pub(crate) tracing: Arc<TracingComponent>,
}

impl<C: Client> Search<C> {
    pub fn new_request(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        headers: Option<HashMap<&str, &str>>,
        body: Option<Bytes>,
    ) -> Request {
        let auth = if let Some(obo) = on_behalf_of {
            Auth::OnBehalfOf(OnBehalfOfInfo {
                username: obo.username,
                password_or_domain: obo.password_or_domain,
            })
        } else {
            self.auth.clone()
        };

        let mut req = Request::new(method, format!("{}/{}", self.endpoint, path.into()))
            .auth(auth)
            .user_agent(self.user_agent.clone())
            .content_type(content_type.into())
            .body(body);

        if let Some(headers) = headers {
            for (key, value) in headers.into_iter() {
                req = req.add_header(key, value);
            }
        }

        req
    }

    pub async fn execute(
        &self,
        method: Method,
        path: impl Into<String>,
        content_type: impl Into<String>,
        on_behalf_of: Option<OnBehalfOfInfo>,
        headers: Option<HashMap<&str, &str>>,
        body: Option<Bytes>,
    ) -> crate::httpx::error::Result<Response> {
        let req = self.new_request(method, path, content_type, on_behalf_of, headers, body);

        self.http_client.execute(req).await
    }

    pub async fn query(&self, opts: &QueryOptions) -> error::Result<SearchRespReader> {
        if !self.vector_search_enabled && (opts.knn.is_some() || opts.knn_operator.is_some()) {
            return Err(error::Error::new_unsupported_feature_error(
                "vector search".to_string(),
            ));
        }

        let req_uri = if let Some(bucket) = &opts.bucket_name {
            if let Some(scope) = &opts.scope_name {
                format!(
                    "api/bucket/{}/scope/{}/index/{}/query",
                    bucket, scope, opts.index_name
                )
            } else {
                return Err(error::Error::new_invalid_argument_error(
                    "must specify both or neither scope and bucket names",
                    None,
                ));
            }
        } else {
            format!("api/index/{}/query", opts.index_name)
        };

        let on_behalf_of = opts.on_behalf_of.clone();

        let body = serde_json::to_vec(&opts).map_err(|e| {
            error::Error::new_encoding_error(format!("could not serialize query options: {e}"))
        })?;

        let peer_addr = get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default();
        let canonical_addr =
            get_host_port_tuple_from_uri(&self.canonical_endpoint).unwrap_or_default();
        let res = self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::new(
                    (&peer_addr.0, &peer_addr.1),
                    (&canonical_addr.0, &canonical_addr.1),
                    None,
                ),
                self.execute(
                    Method::POST,
                    req_uri,
                    "application/json",
                    on_behalf_of,
                    None,
                    Some(Bytes::from(body)),
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        SearchRespReader::new(res, &opts.index_name, &self.endpoint).await
    }

    pub async fn upsert_index(&self, opts: &UpsertIndexOptions<'_>) -> error::Result<()> {
        let req_uri = Self::get_uri(&opts.index.name, opts.bucket_name, opts.scope_name)?;

        let body = serde_json::to_vec(&opts.index).map_err(|e| {
            error::Error::new_encoding_error(format!("could not serialize index: {e}"))
        })?;

        let mut headers = HashMap::new();
        headers.insert("cache-control", "no-cache");

        let res = self
            .execute(
                Method::PUT,
                req_uri,
                "application/json",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                Some(headers),
                Some(Bytes::from(body)),
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(
                decode_response_error(res, opts.index.name.clone(), self.endpoint.clone()).await,
            );
        }

        Ok(())
    }

    pub async fn delete_index(&self, opts: &DeleteIndexOptions<'_>) -> error::Result<()> {
        let req_uri = Self::get_uri(opts.index_name, opts.bucket_name, opts.scope_name)?;

        let mut headers = HashMap::new();
        headers.insert("cache-control", "no-cache");

        let res = self
            .execute(
                Method::DELETE,
                req_uri,
                "application/json",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                Some(headers),
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(
                res,
                opts.index_name.to_string(),
                self.endpoint.clone(),
            )
            .await);
        }

        Ok(())
    }

    pub async fn get_index(&self, opts: &GetIndexOptions<'_>) -> error::Result<Index> {
        let req_uri = Self::get_uri(opts.index_name, opts.bucket_name, opts.scope_name)?;

        let res = self
            .execute(
                Method::GET,
                req_uri,
                "",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(
                res,
                opts.index_name.to_string(),
                self.endpoint.clone(),
            )
            .await);
        }

        let index: SearchIndexResponseJson = parse_response_json(res).await.map_err(|e| {
            error::Error::new_message_error(
                format!("failed to parse index json: {e}"),
                Some(self.endpoint.clone()),
            )
        })?;

        Ok(index.index_def.into())
    }

    pub async fn get_all_indexes(
        &self,
        opts: &GetAllIndexesOptions<'_>,
    ) -> error::Result<Vec<Index>> {
        let req_uri = Self::get_uri("", opts.bucket_name, opts.scope_name)?;

        let res = self
            .execute(
                Method::GET,
                req_uri,
                "",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(res, "".to_string(), self.endpoint.clone()).await);
        }

        let index: SearchIndexesResponseJson = parse_response_json(res).await.map_err(|e| {
            error::Error::new_message_error(
                format!("failed to parse index json: {e}"),
                Some(self.endpoint.clone()),
            )
        })?;

        Ok(index
            .indexes
            .index_defs
            .into_values()
            .map(Index::from)
            .collect())
    }

    pub async fn analyze_document(
        &self,
        opts: &AnalyzeDocumentOptions<'_>,
    ) -> error::Result<DocumentAnalysis> {
        let req_uri = Self::get_uri(opts.index_name, opts.bucket_name, opts.scope_name)?;
        let body = Bytes::from(opts.doc_content.to_vec());

        let res = self
            .execute(
                Method::POST,
                req_uri,
                "application/json",
                // TODO: change when we change ownership on execute
                opts.on_behalf_of.cloned(),
                None,
                Some(body),
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(
                res,
                opts.index_name.to_string(),
                self.endpoint.clone(),
            )
            .await);
        }

        let analysis: DocumentAnalysisJson = parse_response_json(res).await.map_err(|e| {
            error::Error::new_message_error(
                format!("failed to parse document analysis: {e}"),
                Some(self.endpoint.clone()),
            )
        })?;

        Ok(analysis.into())
    }

    pub async fn get_indexed_documents_count(
        &self,
        opts: &GetIndexedDocumentsCountOptions<'_>,
    ) -> error::Result<u64> {
        let req_uri = if opts.scope_name.is_none() && opts.bucket_name.is_none() {
            format!("/api/index/{}/count", opts.index_name)
        } else if opts.scope_name.is_some() && opts.bucket_name.is_some() {
            format!(
                "/api/bucket/{}/scope/{}/index/{}/count",
                opts.bucket_name.unwrap(),
                opts.scope_name.unwrap(),
                opts.index_name
            )
        } else {
            return Err(error::Error::new_invalid_argument_error(
                "must specify both or neither of scope and bucket names",
                None,
            ));
        };

        let res = self
            .execute(
                Method::GET,
                req_uri,
                "",
                opts.on_behalf_of.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(
                res,
                opts.index_name.to_string(),
                self.endpoint.clone(),
            )
            .await);
        }

        let count: IndexedDocumentsJson = parse_response_json(res).await.map_err(|e| {
            error::Error::new_message_error(
                format!("failed to parse indexed count: {e}"),
                Some(self.endpoint.clone()),
            )
        })?;

        Ok(count.count)
    }

    pub async fn pause_ingest(&self, opts: &PauseIngestOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "ingestControl/pause",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn resume_ingest(&self, opts: &ResumeIngestOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "ingestControl/resume",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn allow_querying(&self, opts: &AllowQueryingOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "queryControl/allow",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn disallow_querying(&self, opts: &DisallowQueryingOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "queryControl/disallow",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn freeze_plan(&self, opts: &FreezePlanOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "planFreezeControl/freeze",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn unfreeze_plan(&self, opts: &UnfreezePlanOptions<'_>) -> error::Result<()> {
        self.control_request(
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            "planFreezeControl/unfreeze",
            opts.on_behalf_of,
        )
        .await
    }

    pub async fn ping(&self, opts: &PingOptions<'_>) -> error::Result<()> {
        let peer_addr = get_host_port_tuple_from_uri(&self.endpoint).unwrap_or_default();
        let canonical_addr =
            get_host_port_tuple_from_uri(&self.canonical_endpoint).unwrap_or_default();
        let res = match self
            .tracing
            .orchestrate_dispatch_span(
                BeginDispatchFields::new(
                    (&peer_addr.0, &peer_addr.1),
                    (&canonical_addr.0, &canonical_addr.1),
                    None,
                ),
                self.execute(
                    Method::GET,
                    "/api/ping",
                    "",
                    opts.on_behalf_of.cloned(),
                    None,
                    None,
                ),
                |_| EndDispatchFields::new(None, None),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::new_http_error(e, self.endpoint.to_string()));
            }
        };

        if res.status().is_success() {
            return Ok(());
        }

        Err(Error::new_message_error(
            format!("ping failed with status code: {}", res.status()),
            Some(self.endpoint.clone()),
        ))
    }

    async fn control_request(
        &self,
        index_name: &str,
        bucket_name: Option<&str>,
        scope_name: Option<&str>,
        control: &str,
        on_behalf_of: Option<&OnBehalfOfInfo>,
    ) -> error::Result<()> {
        if index_name.is_empty() {
            return Err(error::Error::new_invalid_argument_error(
                "must specify index name",
                None,
            ));
        }

        let req_uri = if scope_name.is_none() && bucket_name.is_none() {
            format!("/api/index/{index_name}/{control}")
        } else if scope_name.is_some() && bucket_name.is_some() {
            format!(
                "/api/bucket/{}/scope/{}/index/{}/{}",
                bucket_name.unwrap(),
                scope_name.unwrap(),
                index_name,
                control
            )
        } else {
            return Err(error::Error::new_invalid_argument_error(
                "must specify both or neither of scope and bucket names",
                None,
            ));
        };

        let res = self
            .execute(
                Method::POST,
                req_uri,
                "application/json",
                on_behalf_of.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(
                decode_response_error(res, index_name.to_string(), self.endpoint.clone()).await,
            );
        }

        Ok(())
    }

    pub(crate) async fn refresh_config(
        &self,
        opts: &RefreshConfigOptions<'_>,
    ) -> error::Result<()> {
        let res = self
            .execute(
                Method::POST,
                "/api/cfgRefresh",
                "application/json",
                opts.on_behalf_of.cloned(),
                None,
                None,
            )
            .await
            .map_err(|e| error::Error::new_http_error(e, self.endpoint.to_string()))?;

        if res.status() != 200 {
            return Err(decode_response_error(res, "".to_string(), self.endpoint.clone()).await);
        }

        Ok(())
    }

    fn get_uri(
        index_name: &str,
        bucket_name: Option<&str>,
        scope_name: Option<&str>,
    ) -> error::Result<String> {
        if let Some(bucket) = &bucket_name {
            if let Some(scope) = &scope_name {
                Ok(format!(
                    "api/bucket/{}/scope/{}/index/{}",
                    bucket, scope, index_name
                ))
            } else {
                Err(error::Error::new_invalid_argument_error(
                    "must specify both or neither scope and bucket names",
                    None,
                ))
            }
        } else {
            Ok(format!("api/index/{}", index_name))
        }
    }
}

pub(crate) async fn decode_response_error(
    response: Response,
    index_name: String,
    endpoint: String,
) -> error::Error {
    let status = response.status();
    let body = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return Error::new_http_error(e, endpoint);
        }
    };

    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s.to_lowercase(),
        Err(e) => {
            return Error::new_message_error(
                format!("could not parse error response: {e}"),
                endpoint,
            );
        }
    };

    decode_common_error(index_name, status, &body_str, endpoint)
}

pub(crate) fn decode_common_error(
    index_name: String,
    status: StatusCode,
    body_str: &str,
    endpoint: String,
) -> error::Error {
    let error_kind = if status == 401 || status == 403 {
        error::ServerErrorKind::AuthenticationFailure
    } else if body_str.contains("index not found") {
        error::ServerErrorKind::IndexNotFound
    } else if body_str.contains("index with the same name already exists")
        || (body_str.contains("current index uuuid")
            && body_str.contains("did not match input uuid"))
    {
        error::ServerErrorKind::IndexExists
    } else if body_str.contains("unknown indextype") {
        error::ServerErrorKind::UnknownIndexType
    } else if body_str.contains("error obtaining vbucket count for bucket")
        || body_str.contains("requested resource not found")
        || body_str.contains("non existent bucket")
    {
        // In server 7.2.4 and later, ns_server produces "non-existent bucket" instead of "requested resource not found".
        // However in 7.6.0, FTS reordered their handling here and produces the "vbucket count for bucket" instead.
        // So we need to check for all the variants of this.
        error::ServerErrorKind::SourceNotFound
    } else if body_str
        .contains("failed to connect to or retrieve information from source, sourcetype")
    {
        error::ServerErrorKind::SourceTypeIncorrect
    } else if body_str.contains("no planpindexes for indexname") {
        error::ServerErrorKind::NoIndexPartitionsPlanned
    } else if body_str.contains("no local pindexes found") {
        error::ServerErrorKind::NoIndexPartitionsFound
    } else if status == 500 {
        error::ServerErrorKind::Internal
    } else if status == 429 {
        if body_str.contains("num_concurrent_requests")
            || body_str.contains("num_queries_per_min")
            || body_str.contains("ingress_mib_per_min")
            || body_str.contains("egress_mib_per_min")
        {
            error::ServerErrorKind::RateLimitedFailure
        } else {
            error::ServerErrorKind::Unknown
        }
    } else {
        error::ServerErrorKind::Unknown
    };

    error::Error::new_server_error(ServerError::new(
        error_kind, index_name, body_str, endpoint, status,
    ))
}
