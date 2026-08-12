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

//! Full-Text Search (FTS) types for building and executing search requests.
//!
//! Use these types with [`Scope::search`](crate::scope::Scope::search) to execute
//! Full-Text Search queries against a Couchbase FTS index.
//!
//! # Modules
//!
//! - [`queries`] — Search query types (match, term, phrase, boolean, etc.)
//! - [`request`] — [`SearchRequest`](request::SearchRequest) — the top-level request container
//! - [`facets`] — Facet definitions for aggregating search results
//! - [`sort`] — Sort definitions for ordering search results
//! - [`vector`] — Vector search types for similarity-based queries
//! - [`location`] — Geo-location types for geospatial queries

pub mod facets;
pub mod location;
pub mod queries;
pub mod request;
pub mod scoring;
pub mod sort;
pub mod vector;
