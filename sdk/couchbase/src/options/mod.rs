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

//! Configuration options for all SDK operations.
//!
//! Every SDK operation accepts an optional options struct that configures its behavior.
//! All options structs implement `Default` and provide a builder-style API. Pass `None`
//! to any operation to use the defaults.
//!
//! # Modules
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`cluster_options`] | [`ClusterOptions`](cluster_options::ClusterOptions) — connection and cluster configuration |
//! | [`kv_options`] | Options for KV CRUD operations (get, upsert, insert, replace, remove, etc.) |
//! | [`kv_binary_options`] | Options for binary operations (append, prepend, increment, decrement) |
//! | [`query_options`] | [`QueryOptions`](query_options::QueryOptions) — query parameters and consistency |
//! | [`search_options`] | Options for Full-Text Search |
//! | [`diagnostic_options`] | Options for ping, diagnostics, and wait_until_ready |
//! | [`bucket_mgmt_options`] | Options for bucket management |
//! | [`collection_mgmt_options`] | Options for collection/scope management |
//! | [`user_mgmt_options`] | Options for user management |
//! | [`query_index_mgmt_options`] | Options for query index management |
//! | [`search_index_mgmt_options`] | Options for search index management |
//! | [`collection_ds_options`] | Options for data structure operations (list, map, set, queue) |

pub mod bucket_mgmt_options;
pub mod cluster_options;
pub mod collection_ds_options;
pub mod collection_mgmt_options;
pub mod conversational_query_options;
pub mod diagnostic_options;
pub mod kv_binary_options;
pub mod kv_options;
pub mod query_index_mgmt_options;
pub mod query_options;
pub mod search_index_mgmt_options;
pub mod search_options;
pub mod user_mgmt_options;
