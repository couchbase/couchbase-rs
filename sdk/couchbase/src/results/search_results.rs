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

//! Result types for Full-Text Search operations.

use crate::error;
use crate::timeouts::OperationDeadline;
use chrono::{DateTime, FixedOffset};
use couchbase_core::results::search::SearchResultStream;
use couchbase_core::searchx;
use couchbase_core::searchx::search_result::ResultHit;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// The result of a Full-Text Search query.
///
/// Use [`rows`](SearchResult::rows) to stream result rows (hits), and
/// [`metadata`](SearchResult::metadata) to access search metadata.
///
/// # Example
///
/// ```rust,no_run
/// use futures::StreamExt;
/// use couchbase::search::request::SearchRequest;
/// use couchbase::search::queries::{Query, MatchQuery};
///
/// # async fn example(scope: couchbase::scope::Scope) -> couchbase::error::Result<()> {
/// let request = SearchRequest::with_search_query(
///     Query::Match(MatchQuery::new("airport").field("type")),
/// );
///
/// let mut result = scope.search("my-index", request, None).await?;
///
/// // Stream through the rows (hits)
/// let mut rows = result.rows();
/// while let Some(row) = rows.next().await {
///     let row = row?;
///     println!("Hit: id={}, score={}", row.id, row.score);
/// }
/// drop(rows);
///
/// // Access metadata after consuming rows
/// let meta = result.metadata()?;
/// println!("Total hits: {}", meta.metrics.total_hits);
/// # Ok(())
/// # }
/// ```
pub struct SearchResult {
    wrapped: SearchResultStream,
    deadline: Option<OperationDeadline>,
}

/// Performance metrics for a search query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SearchMetrics {
    /// Time taken to execute the search.
    pub took: Duration,
    /// Total number of matching documents.
    pub total_hits: u64,
    /// The highest relevance score among all hits.
    pub max_score: f64,
    /// Number of partitions that succeeded.
    pub successful_partition_count: u64,
    /// Number of partitions that failed.
    pub failed_partition_count: u64,
    /// Total number of partitions queried.
    pub total_partition_count: u64,
}

impl From<&searchx::search_result::Metrics> for SearchMetrics {
    fn from(metrics: &searchx::search_result::Metrics) -> Self {
        Self {
            took: metrics.took,
            total_hits: metrics.total_hits,
            max_score: metrics.max_score,
            successful_partition_count: metrics.successful_partition_count,
            failed_partition_count: metrics.failed_partition_count,
            total_partition_count: metrics.total_partition_count,
        }
    }
}

/// Metadata associated with a search result.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SearchMetaData<'a> {
    /// Errors keyed by partition name, if any.
    pub errors: &'a HashMap<String, String>,
    /// Performance metrics for the search.
    pub metrics: SearchMetrics,
}

impl<'a> From<&'a searchx::search_result::MetaData> for SearchMetaData<'a> {
    fn from(meta: &'a searchx::search_result::MetaData) -> Self {
        Self {
            errors: &meta.errors,
            metrics: SearchMetrics::from(&meta.metrics),
        }
    }
}

/// A term facet result entry.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct TermFacetResult<'a> {
    /// The term value.
    pub term: &'a str,
    /// The number of documents matching this term.
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::TermFacetResult> for TermFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::TermFacetResult) -> Self {
        Self {
            term: &facet.term,
            count: facet.count,
        }
    }
}

/// A numeric range facet result entry.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
#[non_exhaustive]
pub struct NumericRangeFacetResult<'a> {
    /// The name of the range bucket.
    pub name: &'a str,
    /// The minimum value of the range.
    pub min: f64,
    /// The maximum value of the range.
    pub max: f64,
    /// The number of documents in this range.
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::NumericRangeFacetResult> for NumericRangeFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::NumericRangeFacetResult) -> Self {
        Self {
            name: &facet.name,
            min: facet.min,
            max: facet.max,
            count: facet.count,
        }
    }
}

/// A date range facet result entry.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct DateRangeFacetResult<'a> {
    /// The name of the date range bucket.
    pub name: &'a str,
    /// The start date/time of the range.
    pub start: &'a DateTime<FixedOffset>,
    /// The end date/time of the range.
    pub end: &'a DateTime<FixedOffset>,
    /// The number of documents in this range.
    pub count: i64,
}

impl<'a> From<&'a searchx::search_result::DateRangeFacetResult> for DateRangeFacetResult<'a> {
    fn from(facet: &'a searchx::search_result::DateRangeFacetResult) -> Self {
        Self {
            name: &facet.name,
            start: &facet.start,
            end: &facet.end,
            count: facet.count,
        }
    }
}

/// The type of facet result — term, numeric range, or date range.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
#[non_exhaustive]
pub enum SearchFacetResultType<'a> {
    /// Term facet results.
    TermFacets(Vec<TermFacetResult<'a>>),
    /// Numeric range facet results.
    NumericRangeFacets(Vec<NumericRangeFacetResult<'a>>),
    /// Date range facet results.
    DateRangeFacets(Vec<DateRangeFacetResult<'a>>),
}

/// A single facet result from a search query.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
#[non_exhaustive]
pub struct SearchFacetResult<'a> {
    /// The name of the facet.
    pub name: &'a str,
    /// The field that was aggregated.
    pub field: &'a str,
    /// The total number of facet entries.
    pub total: u64,
    /// The number of documents that did not have a value for this facet.
    pub missing: u64,
    /// The number of documents that had a value but were not included in any bucket.
    pub other: u64,
    /// The facet result entries, keyed by type.
    pub facets: SearchFacetResultType<'a>,
}

/// The location of a matched term within a search result document.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct SearchRowLocation {
    /// The field name where the term was found.
    pub field: String,
    /// The matched term.
    pub term: String,
    /// The byte offset where the term starts.
    pub start: u32,
    /// The byte offset where the term ends.
    pub end: u32,
    /// The ordinal position of the term.
    pub position: u32,
    /// Array positions, if the field is an array.
    pub array_positions: Option<Vec<u32>>,
}

/// A collection of term locations within a search result row.
///
/// Provides methods to query locations by field, term, or both.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchRowLocations {
    locations: Vec<SearchRowLocation>,
}

impl SearchRowLocations {
    /// Returns all locations.
    pub fn get_all(&self) -> &[SearchRowLocation] {
        &self.locations
    }

    /// Returns locations matching the given field name.
    pub fn get_by_field(&self, field: &str) -> Vec<&SearchRowLocation> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field)
            .collect()
    }

    /// Returns locations matching the given field name and term.
    pub fn get_by_field_and_term(&self, field: &str, term: &str) -> Vec<&SearchRowLocation> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field && loc.term == term)
            .collect()
    }

    /// Returns the distinct field names across all locations.
    pub fn fields(&self) -> Vec<&str> {
        self.locations
            .iter()
            .map(|loc| loc.field.as_str())
            .collect()
    }

    /// Returns the distinct terms across all locations.
    pub fn terms(&self) -> Vec<&str> {
        self.locations.iter().map(|loc| loc.term.as_str()).collect()
    }

    /// Returns the terms for a specific field.
    pub fn terms_for(&self, field: &str) -> Vec<&str> {
        self.locations
            .iter()
            .filter(|loc| loc.field == field)
            .map(|loc| loc.term.as_str())
            .collect()
    }
}

/// A single row (hit) in a search result.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SearchRow {
    /// The index the hit came from.
    pub index: String,
    /// The document ID of the hit.
    pub id: String,
    /// The relevance score of the hit.
    pub score: f64,
    /// The query execution explanation, if requested.
    pub explanation: Option<Box<RawValue>>,
    /// The term locations within the document, if requested.
    pub locations: Option<SearchRowLocations>,
    /// The highlighted fragments, if highlighting was requested.
    pub fragments: Option<HashMap<String, Vec<String>>>,
    fields: Option<Box<RawValue>>,
}

impl SearchRow {
    /// Deserializes the stored fields of this search hit into the requested type.
    ///
    /// Returns an error if no fields were returned or deserialization fails.
    pub fn fields<V: DeserializeOwned>(&self) -> error::Result<V> {
        if let Some(fields) = &self.fields {
            serde_json::from_str(fields.get()).map_err(error::Error::decoding_failure_from_serde)
        } else {
            Err(error::Error::other_failure("no fields in response"))
        }
    }
}

impl From<ResultHit> for SearchRow {
    fn from(hit: ResultHit) -> Self {
        let locations = if let Some(hit_locations) = hit.locations {
            let mut locations = vec![];
            for (field_name, field_data) in hit_locations {
                for (term_name, term_data) in field_data {
                    for (loc_idx, loc_data) in term_data.into_iter().enumerate() {
                        locations.push(SearchRowLocation {
                            field: field_name.clone(),
                            term: term_name.clone(),
                            start: loc_data.start,
                            end: loc_data.end,
                            position: loc_idx as u32,
                            array_positions: loc_data.array_positions,
                        });
                    }
                }
            }

            Some(SearchRowLocations { locations })
        } else {
            None
        };

        Self {
            index: hit.index,
            id: hit.id,
            score: hit.score,
            explanation: hit.explanation,
            locations,
            fragments: hit.fragments,
            fields: hit.fields,
        }
    }
}

impl From<SearchResultStream> for SearchResult {
    fn from(wrapped: SearchResultStream) -> Self {
        Self {
            wrapped,
            deadline: None,
        }
    }
}

impl SearchResult {
    pub(crate) fn with_deadline(wrapped: SearchResultStream, deadline: OperationDeadline) -> Self {
        Self {
            wrapped,
            deadline: Some(deadline),
        }
    }
}

struct SearchRows<'a> {
    wrapped: &'a mut SearchResultStream,
    deadline: Option<&'a mut OperationDeadline>,
}

impl Stream for SearchRows<'_> {
    type Item = error::Result<SearchRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(deadline) = self.deadline.as_mut() {
            if let Poll::Ready(res) = deadline.poll(cx) {
                return Poll::Ready(res.err().map(Err));
            }
        }

        let row = match self.wrapped.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row))) => row,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => {
                if let Some(deadline) = self.deadline.as_mut() {
                    deadline.disarm();
                }
                return Poll::Ready(None);
            }
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(Some(Ok(row.into())))
    }
}

impl SearchResult {
    /// Returns the metadata for this search result.
    ///
    /// Metadata includes performance metrics and any per-partition errors.
    pub fn metadata(&self) -> error::Result<SearchMetaData<'_>> {
        Ok(self.wrapped.metadata()?.into())
    }

    /// Returns a stream of search result rows (hits).
    ///
    /// Each item in the stream is a [`SearchRow`] containing the document ID,
    /// score, and any requested fields, locations, or fragments.
    pub fn rows(&mut self) -> impl Stream<Item = error::Result<SearchRow>> + '_ {
        SearchRows {
            wrapped: &mut self.wrapped,
            deadline: self.deadline.as_mut(),
        }
    }

    /// Returns the facet results for this search, keyed by facet name.
    pub fn facets(&self) -> error::Result<HashMap<&String, SearchFacetResult<'_>>> {
        let mut facets = HashMap::new();
        for (name, facet) in self.wrapped.facets()? {
            if let Some(facet_terms) = &facet.terms {
                let mut terms = vec![];
                for term in facet_terms {
                    terms.push(TermFacetResult::from(term));
                }

                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::TermFacets(terms),
                    },
                );
            } else if let Some(facet_dates) = &facet.date_ranges {
                let mut dates = vec![];
                for date in facet_dates {
                    dates.push(DateRangeFacetResult::from(date));
                }
                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::DateRangeFacets(dates),
                    },
                );
            } else if let Some(numeric) = &facet.numeric_ranges {
                let mut range = vec![];
                for num in numeric {
                    range.push(NumericRangeFacetResult::from(num));
                }
                facets.insert(
                    name,
                    SearchFacetResult {
                        name,
                        field: &facet.field,
                        total: facet.total as u64,
                        missing: facet.missing as u64,
                        other: facet.other as u64,
                        facets: SearchFacetResultType::NumericRangeFacets(range),
                    },
                );
            }
        }

        Ok(facets)
    }
}
