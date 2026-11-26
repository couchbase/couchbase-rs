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

//! Result types returned by SDK operations.
//!
//! Each operation returns a specific result type containing the operation's output
//! (e.g. document content, CAS value, mutation tokens).
//!
//! # Modules
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`kv_results`] | Results for KV operations ([`GetResult`](kv_results::GetResult), [`MutationResult`](kv_results::MutationResult), etc.) |
//! | [`kv_binary_results`] | Results for binary operations ([`CounterResult`](kv_binary_results::CounterResult)) |
//! | [`query_results`] | Results for SQL++ queries ([`QueryResult`](query_results::QueryResult)) |
//! | [`search_results`] | Results for Full-Text Search |
//! | [`diagnostics`] | Results for diagnostics and ping operations |
//! | [`collections_mgmt_results`] | Results for collection management |
//! | [`query_index_mgmt_results`] | Results for query index management |

pub mod collections_mgmt_results;
pub mod conversational_query_results;
pub mod diagnostics;
pub mod kv_binary_results;
pub mod kv_results;
pub(crate) mod projection;
pub mod query_index_mgmt_results;
pub mod query_results;
pub mod search_results;
