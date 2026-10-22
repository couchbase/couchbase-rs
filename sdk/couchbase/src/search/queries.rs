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

//! Full-Text Search query types.
//!
//! This module provides the various search query types that can be used with
//! [`SearchRequest`](crate::search::request::SearchRequest). All query types
//! are constructed via the [`Query`] builder enum.
//!
//! # Common Query Types
//!
//! | Type | Description |
//! |------|-------------|
//! | [`MatchQuery`] | Analyzes input text and matches it against an index |
//! | [`MatchPhraseQuery`] | Matches an exact phrase in the index |
//! | [`TermQuery`] | Matches a specific term without analysis |
//! | [`PrefixQuery`] | Matches terms with a given prefix |
//! | [`RegexpQuery`] | Matches terms against a regular expression |
//! | [`WildcardQuery`] | Matches terms using wildcards (`*`, `?`) |
//! | [`BooleanQuery`] | Combines queries with must/should/must-not logic |
//! | [`QueryStringQuery`] | Parses a human-readable query string |
//! | [`MatchAllQuery`] | Matches all documents |
//! | [`MatchNoneQuery`] | Matches no documents |
//! | [`CustomScoreQuery`] | Re-scores results using a user-defined JavaScript function |
//! | [`CustomFilterQuery`] | Filters results using a user-defined JavaScript function |

use crate::search::location::Location;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;

/// The boolean operator for combining match query terms.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MatchOperator {
    /// Any term may match (default).
    Or,
    /// All terms must match.
    And,
}

impl TryFrom<&str> for MatchOperator {
    type Error = crate::error::Error;

    fn try_from(operator: &str) -> Result<Self, Self::Error> {
        match operator {
            "or" => Ok(MatchOperator::Or),
            "and" => Ok(MatchOperator::And),
            "OR" => Ok(MatchOperator::Or),
            "AND" => Ok(MatchOperator::And),
            _ => Err(crate::error::Error::invalid_argument(
                "operator",
                "invalid match operator",
            )),
        }
    }
}

impl TryFrom<String> for MatchOperator {
    type Error = crate::error::Error;

    fn try_from(operator: String) -> Result<Self, Self::Error> {
        Self::try_from(operator.as_str())
    }
}

impl Display for MatchOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchOperator::Or => write!(f, "or"),
            MatchOperator::And => write!(f, "and"),
        }
    }
}

/// A full-text match query that analyzes input text and matches it against the index.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchQuery {
    /// The analyzer to use for this query.
    pub analyzer: Option<String>,
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The fuzziness level for the query.
    pub fuzziness: Option<u64>,
    /// The input text to match.
    pub match_input: String,
    /// The boolean operator for combining terms in the query.
    pub operator: Option<MatchOperator>,
    /// The prefix length for the query.
    pub prefix_length: Option<u64>,
}

impl MatchQuery {
    /// Creates a new `MatchQuery` with the given input text.
    pub fn new(match_input: impl Into<String>) -> Self {
        Self {
            analyzer: None,
            boost: None,
            field: None,
            fuzziness: None,
            match_input: match_input.into(),
            operator: None,
            prefix_length: None,
        }
    }

    /// Sets the analyzer for the query.
    pub fn analyzer(mut self, analyzer: impl Into<String>) -> Self {
        self.analyzer = Some(analyzer.into());
        self
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Sets the fuzziness level for the query.
    pub fn fuzziness(mut self, fuzziness: u64) -> Self {
        self.fuzziness = Some(fuzziness);
        self
    }

    /// Sets the boolean operator for combining terms in the query.
    pub fn operator(mut self, operator: impl Into<MatchOperator>) -> Self {
        self.operator = Some(operator.into());
        self
    }

    /// Sets the prefix length for the query.
    pub fn prefix_length(mut self, prefix_length: u64) -> Self {
        self.prefix_length = Some(prefix_length);
        self
    }
}

/// A full-text match phrase query that matches an exact phrase in the index.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchPhraseQuery {
    /// The analyzer to use for this query.
    pub analyzer: Option<String>,
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The exact phrase to match.
    pub match_phrase: String,
}

impl MatchPhraseQuery {
    /// Creates a new `MatchPhraseQuery` with the given phrase.
    pub fn new(match_phrase: impl Into<String>) -> Self {
        Self {
            analyzer: None,
            boost: None,
            field: None,
            match_phrase: match_phrase.into(),
        }
    }

    /// Sets the analyzer for the query.
    pub fn analyzer(mut self, analyzer: impl Into<String>) -> Self {
        self.analyzer = Some(analyzer.into());
        self
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A regular expression query that matches terms against a regular expression.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RegexpQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The regular expression to match.
    pub regexp: String,
}

impl RegexpQuery {
    /// Creates a new `RegexpQuery` with the given regular expression.
    pub fn new(regexp: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            regexp: regexp.into(),
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A query that parses a human-readable query string.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct QueryStringQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The query string to parse.
    pub query: String,
}

impl QueryStringQuery {
    /// Creates a new `QueryStringQuery` with the given query string.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            boost: None,
            query: query.into(),
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

/// A numeric range query that matches documents with numeric values within a specified range.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericRangeQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// Whether the minimum value is inclusive.
    pub inclusive_min: Option<bool>,
    /// Whether the maximum value is inclusive.
    pub inclusive_max: Option<bool>,
    /// The minimum value of the range.
    pub min: Option<f32>,
    /// The maximum value of the range.
    pub max: Option<f32>,
}

impl NumericRangeQuery {
    /// Creates a new `NumericRangeQuery`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Sets the minimum value of the range.
    pub fn inclusive_min(mut self, min: f32, inclusive_min: bool) -> Self {
        self.min = Some(min);
        self.inclusive_min = Some(inclusive_min);
        self
    }

    /// Sets the maximum value of the range.
    pub fn inclusive_max(mut self, max: f32, inclusive_max: bool) -> Self {
        self.max = Some(max);
        self.inclusive_max = Some(inclusive_max);
        self
    }

    /// Sets the minimum value of the range.
    pub fn min(mut self, min: f32) -> Self {
        self.min = Some(min);
        self
    }

    /// Sets the maximum value of the range.
    pub fn max(mut self, max: f32) -> Self {
        self.max = Some(max);
        self
    }
}

/// A date range query that matches documents with date values within a specified range.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct DateRangeQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The date/time parser to use.
    pub datetime_parser: Option<String>,
    /// The end date/time of the range.
    pub end: Option<String>,
    /// Whether the start date/time is inclusive.
    pub inclusive_start: Option<bool>,
    /// Whether the end date/time is inclusive.
    pub inclusive_end: Option<bool>,
    /// The start date/time of the range.
    pub start: Option<String>,
}

impl DateRangeQuery {
    /// Creates a new `DateRangeQuery`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Sets the date/time parser to use.
    pub fn datetime_parser(mut self, datetime_parser: impl Into<String>) -> Self {
        self.datetime_parser = Some(datetime_parser.into());
        self
    }

    /// Sets the start date/time of the range.
    pub fn inclusive_start(mut self, start: impl Into<String>, inclusive_start: bool) -> Self {
        self.start = Some(start.into());
        self.inclusive_start = Some(inclusive_start);
        self
    }

    /// Sets the end date/time of the range.
    pub fn inclusive_end(mut self, end: impl Into<String>, inclusive_end: bool) -> Self {
        self.end = Some(end.into());
        self.inclusive_end = Some(inclusive_end);
        self
    }

    /// Sets the end date/time of the range.
    pub fn end(mut self, end: impl Into<String>) -> Self {
        self.end = Some(end.into());
        self
    }

    /// Sets the start date/time of the range.
    pub fn start(mut self, start: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self
    }
}

/// A term range query that matches documents with terms within a specified range.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct TermRangeQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// Whether the minimum term is inclusive.
    pub inclusive_min: Option<bool>,
    /// Whether the maximum term is inclusive.
    pub inclusive_max: Option<bool>,
    /// The maximum term of the range.
    pub max: Option<String>,
    /// The minimum term of the range.
    pub min: Option<String>,
}

impl TermRangeQuery {
    /// Creates a new `TermRangeQuery`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Sets the minimum term of the range.
    pub fn inclusive_min(mut self, min: impl Into<String>, inclusive_min: bool) -> Self {
        self.min = Some(min.into());
        self.inclusive_min = Some(inclusive_min);
        self
    }

    /// Sets the maximum term of the range.
    pub fn inclusive_max(mut self, max: impl Into<String>, inclusive_max: bool) -> Self {
        self.max = Some(max.into());
        self.inclusive_max = Some(inclusive_max);
        self
    }

    /// Sets the maximum term of the range.
    pub fn max(mut self, max: impl Into<String>) -> Self {
        self.max = Some(max.into());
        self
    }

    /// Sets the minimum term of the range.
    pub fn min(mut self, min: impl Into<String>) -> Self {
        self.min = Some(min.into());
        self
    }
}

/// A conjunction query that matches documents matching all of the conjuncts.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConjunctionQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The conjuncts of the query.
    pub conjuncts: Vec<Query>,
}

impl ConjunctionQuery {
    /// Creates a new `ConjunctionQuery` with the given conjuncts.
    pub fn new(conjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            conjuncts,
        }
    }

    /// Adds a conjunct to the query.
    pub fn and(mut self, query: Query) -> Self {
        self.conjuncts.push(query);
        self
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

/// A disjunction query that matches documents matching any of the disjuncts.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DisjunctionQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The disjuncts of the query.
    pub disjuncts: Vec<Query>,
    /// The minimum number of disjuncts that must match.
    pub min: Option<u32>,
}

impl DisjunctionQuery {
    /// Creates a new `DisjunctionQuery` with the given disjuncts.
    pub fn new(disjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            disjuncts,
            min: None,
        }
    }

    /// Adds a disjunct to the query.
    pub fn or(mut self, query: Query) -> Self {
        self.disjuncts.push(query);
        self
    }

    /// Sets the minimum number of disjuncts that must match.
    pub fn min(mut self, min: u32) -> Self {
        self.min = Some(min);
        self
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

/// A boolean query that combines multiple queries with must/should/must-not logic.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct BooleanQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The conjunction query for mandatory clauses.
    pub must: Option<ConjunctionQuery>,
    /// The disjunction query for prohibited clauses.
    pub must_not: Option<DisjunctionQuery>,
    /// The disjunction query for optional clauses.
    pub should: Option<DisjunctionQuery>,
}

impl BooleanQuery {
    /// Creates a new `BooleanQuery`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the conjunction query for mandatory clauses.
    pub fn must(mut self, must: ConjunctionQuery) -> Self {
        self.must = Some(must);
        self
    }

    /// Sets the disjunction query for prohibited clauses.
    pub fn must_not(mut self, must_not: DisjunctionQuery) -> Self {
        self.must_not = Some(must_not);
        self
    }

    /// Sets the disjunction query for optional clauses.
    pub fn should(mut self, should: DisjunctionQuery) -> Self {
        self.should = Some(should);
        self
    }
}

/// A wildcard query that matches terms using wildcards (`*`, `?`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WildcardQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The wildcard pattern to match.
    pub wildcard: String,
}

impl WildcardQuery {
    /// Creates a new `WildcardQuery` with the given wildcard pattern.
    pub fn new(wildcard: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            wildcard: wildcard.into(),
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A query that matches documents by their document IDs.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct DocIDQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The document IDs to match.
    pub ids: Vec<String>,
}

impl DocIDQuery {
    /// Creates a new `DocIDQuery` with the given document IDs.
    pub fn new(ids: Vec<String>) -> Self {
        Self { boost: None, ids }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }
}

/// A boolean field query that matches documents with a specific boolean value.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BooleanFieldQuery {
    /// The boolean value to match.
    pub bool_value: bool,
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
}

impl BooleanFieldQuery {
    /// Creates a new `BooleanFieldQuery` with the given boolean value.
    pub fn new(bool_value: bool) -> Self {
        Self {
            bool_value,
            boost: None,
            field: None,
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A term query that matches documents with a specific term.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TermQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The fuzziness level for the query.
    pub fuzziness: Option<u32>,
    /// The prefix length for the query.
    pub prefix_length: Option<u32>,
    /// The term to match.
    pub term: String,
}

impl TermQuery {
    /// Creates a new `TermQuery` with the given term.
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            fuzziness: None,
            prefix_length: None,
            term: term.into(),
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    /// Sets the fuzziness level for the query.
    pub fn fuzziness(mut self, fuzziness: u32) -> Self {
        self.fuzziness = Some(fuzziness);
        self
    }

    /// Sets the prefix length for the query.
    pub fn prefix_length(mut self, prefix_length: u32) -> Self {
        self.prefix_length = Some(prefix_length);
        self
    }
}

/// A phrase query that matches documents containing the specified sequence of terms.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PhraseQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The terms that must appear in the specified order.
    pub terms: Vec<String>,
}

impl PhraseQuery {
    /// Creates a new `PhraseQuery` with the given terms.
    pub fn new(terms: Vec<String>) -> Self {
        Self {
            boost: None,
            field: None,
            terms,
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A prefix query that matches documents with terms that have the specified prefix.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PrefixQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The prefix to match.
    pub prefix: String,
}

impl PrefixQuery {
    /// Creates a new `PrefixQuery` with the given prefix.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            prefix: prefix.into(),
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A query that matches all documents.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchAllQuery {}

impl MatchAllQuery {
    /// Creates a new `MatchAllQuery`.
    pub fn new() -> Self {
        Default::default()
    }
}

/// A query that matches no documents.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchNoneQuery {}

impl MatchNoneQuery {
    /// Creates a new `MatchNoneQuery`.
    pub fn new() -> Self {
        Default::default()
    }
}

/// A geo distance query that matches documents within a specified distance from a location.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoDistanceQuery {
    /// The distance from the location.
    pub distance: String,
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The location to match.
    pub location: Location,
}

impl GeoDistanceQuery {
    /// Creates a new `GeoDistanceQuery` with the given distance and location.
    pub fn new(distance: impl Into<String>, location: Location) -> Self {
        Self {
            distance: distance.into(),
            boost: None,
            field: None,
            location,
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A geo bounding box query that matches documents within a specified bounding box.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoBoundingBoxQuery {
    /// The bottom right location of the bounding box.
    pub bottom_right: Location,
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The top left location of the bounding box.
    pub top_left: Location,
}

impl GeoBoundingBoxQuery {
    /// Creates a new `GeoBoundingBoxQuery` with the given top left and bottom right locations.
    pub fn new(bottom_right: Location, top_left: Location) -> Self {
        Self {
            bottom_right,
            boost: None,
            field: None,
            top_left,
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A geo polygon query that matches documents within a specified polygon.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeoPolygonQuery {
    /// The boost value for this query.
    pub boost: Option<f32>,
    /// The field to query.
    pub field: Option<String>,
    /// The points that define the polygon.
    pub polygon_points: Vec<Location>,
}

impl GeoPolygonQuery {
    /// Creates a new `GeoPolygonQuery` with the given polygon points.
    pub fn new(polygon_points: Vec<Location>) -> Self {
        Self {
            boost: None,
            field: None,
            polygon_points,
        }
    }

    /// Sets the boost value for the query.
    pub fn boost(mut self, boost: f32) -> Self {
        self.boost = Some(boost);
        self
    }

    /// Sets the field to query.
    pub fn field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// A query that scores the results of an inner query using a user-defined JavaScript function.
///
/// Requires a cluster that supports custom script queries.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CustomScoreQuery {
    /// The inner query whose results are passed to the function.
    pub query: Box<Query>,
    /// The JavaScript scoring function.
    pub source: String,
    /// The document fields made available to the function. Use `"*"` for all fields.
    pub fields: Option<Vec<String>>,
    /// The parameters passed to the function.
    pub params: Option<HashMap<String, Value>>,
}

impl CustomScoreQuery {
    /// Creates a new `CustomScoreQuery` with the given inner query and JavaScript function.
    pub fn new(query: Query, source: impl Into<String>) -> Self {
        Self {
            query: Box::new(query),
            source: source.into(),
            fields: None,
            params: None,
        }
    }

    /// Sets the document fields made available to the function. Use `"*"` for all fields.
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Sets the parameters passed to the function.
    pub fn params(mut self, params: impl Into<HashMap<String, Value>>) -> Self {
        self.params = Some(params.into());
        self
    }
}

/// A query that filters the results of an inner query using a user-defined JavaScript function.
///
/// Requires a cluster that supports custom script queries.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CustomFilterQuery {
    /// The inner query whose results are passed to the function.
    pub query: Box<Query>,
    /// The JavaScript filtering function.
    pub source: String,
    /// The document fields made available to the function. Use `"*"` for all fields.
    pub fields: Option<Vec<String>>,
    /// The parameters passed to the function.
    pub params: Option<HashMap<String, Value>>,
}

impl CustomFilterQuery {
    /// Creates a new `CustomFilterQuery` with the given inner query and JavaScript function.
    pub fn new(query: Query, source: impl Into<String>) -> Self {
        Self {
            query: Box::new(query),
            source: source.into(),
            fields: None,
            params: None,
        }
    }

    /// Sets the document fields made available to the function. Use `"*"` for all fields.
    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Sets the parameters passed to the function.
    pub fn params(mut self, params: impl Into<HashMap<String, Value>>) -> Self {
        self.params = Some(params.into());
        self
    }
}

/// The query builder enum for constructing search queries.
///
/// Each variant wraps a specific query type. Use this enum when passing a query to
/// [`SearchRequest::with_search_query`](crate::search::request::SearchRequest::with_search_query)
/// or combining queries inside [`BooleanQuery`], [`ConjunctionQuery`], or [`DisjunctionQuery`].
///
/// # Example
///
/// ```rust
/// use couchbase::search::queries::{Query, MatchQuery, BooleanQuery, ConjunctionQuery, DisjunctionQuery};
///
/// // Simple match query
/// let q = Query::Match(MatchQuery::new("airport").field("type"));
///
/// // Boolean combination
/// let bool_q = Query::Boolean(
///     BooleanQuery::new()
///         .must(ConjunctionQuery::new(vec![
///             Query::Match(MatchQuery::new("hotel").field("type")),
///         ]))
///         .should(DisjunctionQuery::new(vec![
///             Query::Match(MatchQuery::new("pool").field("description")),
///         ])),
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Query {
    /// A full-text match query. See [`MatchQuery`].
    Match(MatchQuery),
    /// A full-text match phrase query. See [`MatchPhraseQuery`].
    MatchPhrase(MatchPhraseQuery),
    /// A regular expression query. See [`RegexpQuery`].
    Regexp(RegexpQuery),
    /// A human-readable query string query. See [`QueryStringQuery`].
    #[allow(clippy::enum_variant_names)]
    QueryString(QueryStringQuery),
    /// A numeric range query. See [`NumericRangeQuery`].
    NumericRange(NumericRangeQuery),
    /// A date range query. See [`DateRangeQuery`].
    DateRange(DateRangeQuery),
    /// A term range query. See [`TermRangeQuery`].
    TermRange(TermRangeQuery),
    /// A conjunction (AND) query. See [`ConjunctionQuery`].
    Conjunction(ConjunctionQuery),
    /// A disjunction (OR) query. See [`DisjunctionQuery`].
    Disjunction(DisjunctionQuery),
    /// A boolean must/should/must-not query. See [`BooleanQuery`].
    Boolean(BooleanQuery),
    /// A wildcard query. See [`WildcardQuery`].
    Wildcard(WildcardQuery),
    /// A document ID query. See [`DocIDQuery`].
    DocID(DocIDQuery),
    /// A boolean field query. See [`BooleanFieldQuery`].
    BooleanField(BooleanFieldQuery),
    /// An exact term query. See [`TermQuery`].
    Term(TermQuery),
    /// An exact phrase query. See [`PhraseQuery`].
    Phrase(PhraseQuery),
    /// A prefix query. See [`PrefixQuery`].
    Prefix(PrefixQuery),
    /// Matches all documents. See [`MatchAllQuery`].
    MatchAll(MatchAllQuery),
    /// Matches no documents. See [`MatchNoneQuery`].
    MatchNone(MatchNoneQuery),
    /// A geo-distance query. See [`GeoDistanceQuery`].
    GeoDistance(GeoDistanceQuery),
    /// A geo bounding box query. See [`GeoBoundingBoxQuery`].
    GeoBoundingBox(GeoBoundingBoxQuery),
    /// A geo polygon query. See [`GeoPolygonQuery`].
    GeoPolygon(GeoPolygonQuery),
    /// A custom score query. See [`CustomScoreQuery`].
    CustomScore(CustomScoreQuery),
    /// A custom filter query. See [`CustomFilterQuery`].
    CustomFilter(CustomFilterQuery),
}

impl From<MatchOperator> for couchbase_core::searchx::queries::MatchOperator {
    fn from(operator: MatchOperator) -> Self {
        match operator {
            MatchOperator::Or => couchbase_core::searchx::queries::MatchOperator::Or,
            MatchOperator::And => couchbase_core::searchx::queries::MatchOperator::And,
        }
    }
}

impl From<MatchOperator> for Option<couchbase_core::searchx::queries::MatchOperator> {
    fn from(operator: MatchOperator) -> Self {
        match operator {
            MatchOperator::Or => Some(couchbase_core::searchx::queries::MatchOperator::Or),
            MatchOperator::And => Some(couchbase_core::searchx::queries::MatchOperator::And),
        }
    }
}

impl From<MatchQuery> for couchbase_core::searchx::queries::MatchQuery {
    fn from(query: MatchQuery) -> Self {
        couchbase_core::searchx::queries::MatchQuery::new(query.match_input)
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .operator(query.operator.map(|o| o.into()))
            .prefix_length(query.prefix_length)
    }
}

impl From<MatchPhraseQuery> for couchbase_core::searchx::queries::MatchPhraseQuery {
    fn from(query: MatchPhraseQuery) -> Self {
        couchbase_core::searchx::queries::MatchPhraseQuery::new(query.match_phrase)
            .analyzer(query.analyzer)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<RegexpQuery> for couchbase_core::searchx::queries::RegexpQuery {
    fn from(query: RegexpQuery) -> Self {
        couchbase_core::searchx::queries::RegexpQuery::new(query.regexp)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<QueryStringQuery> for couchbase_core::searchx::queries::QueryStringQuery {
    fn from(query: QueryStringQuery) -> Self {
        couchbase_core::searchx::queries::QueryStringQuery::new(query.query).boost(query.boost)
    }
}

impl From<NumericRangeQuery> for couchbase_core::searchx::queries::NumericRangeQuery {
    fn from(query: NumericRangeQuery) -> Self {
        couchbase_core::searchx::queries::NumericRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .min(query.min)
            .max(query.max)
    }
}

impl From<DateRangeQuery> for couchbase_core::searchx::queries::DateRangeQuery {
    fn from(query: DateRangeQuery) -> Self {
        couchbase_core::searchx::queries::DateRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .datetime_parser(query.datetime_parser)
            .end(query.end)
            .inclusive_start(query.inclusive_start)
            .inclusive_end(query.inclusive_end)
            .start(query.start)
    }
}

impl From<TermRangeQuery> for couchbase_core::searchx::queries::TermRangeQuery {
    fn from(query: TermRangeQuery) -> Self {
        couchbase_core::searchx::queries::TermRangeQuery::new()
            .boost(query.boost)
            .field(query.field)
            .inclusive_min(query.inclusive_min)
            .inclusive_max(query.inclusive_max)
            .max(query.max)
            .min(query.min)
    }
}

impl From<ConjunctionQuery> for couchbase_core::searchx::queries::ConjunctionQuery {
    fn from(query: ConjunctionQuery) -> Self {
        couchbase_core::searchx::queries::ConjunctionQuery::new(
            query
                .conjuncts
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
        )
        .boost(query.boost)
    }
}

impl From<DisjunctionQuery> for couchbase_core::searchx::queries::DisjunctionQuery {
    fn from(query: DisjunctionQuery) -> Self {
        couchbase_core::searchx::queries::DisjunctionQuery::new(
            query
                .disjuncts
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::queries::Query>>(),
        )
        .boost(query.boost)
        .min(query.min)
    }
}

impl From<BooleanQuery> for couchbase_core::searchx::queries::BooleanQuery {
    fn from(query: BooleanQuery) -> Self {
        couchbase_core::searchx::queries::BooleanQuery::new()
            .boost(query.boost)
            .must(query.must.map(Into::into))
            .must_not(query.must_not.map(Into::into))
            .should(query.should.map(Into::into))
    }
}

impl From<WildcardQuery> for couchbase_core::searchx::queries::WildcardQuery {
    fn from(query: WildcardQuery) -> Self {
        couchbase_core::searchx::queries::WildcardQuery::new(query.wildcard)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<DocIDQuery> for couchbase_core::searchx::queries::DocIDQuery {
    fn from(query: DocIDQuery) -> Self {
        couchbase_core::searchx::queries::DocIDQuery::new(query.ids).boost(query.boost)
    }
}

impl From<BooleanFieldQuery> for couchbase_core::searchx::queries::BooleanFieldQuery {
    fn from(query: BooleanFieldQuery) -> Self {
        couchbase_core::searchx::queries::BooleanFieldQuery::new(query.bool_value)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<TermQuery> for couchbase_core::searchx::queries::TermQuery {
    fn from(query: TermQuery) -> Self {
        couchbase_core::searchx::queries::TermQuery::new(query.term)
            .boost(query.boost)
            .field(query.field)
            .fuzziness(query.fuzziness)
            .prefix_length(query.prefix_length)
    }
}

impl From<PhraseQuery> for couchbase_core::searchx::queries::PhraseQuery {
    fn from(query: PhraseQuery) -> Self {
        couchbase_core::searchx::queries::PhraseQuery::new(query.terms)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<PrefixQuery> for couchbase_core::searchx::queries::PrefixQuery {
    fn from(query: PrefixQuery) -> Self {
        couchbase_core::searchx::queries::PrefixQuery::new(query.prefix)
            .boost(query.boost)
            .field(query.field)
    }
}

impl From<MatchAllQuery> for couchbase_core::searchx::queries::MatchAllQuery {
    fn from(_: MatchAllQuery) -> Self {
        couchbase_core::searchx::queries::MatchAllQuery::new()
    }
}

impl From<MatchNoneQuery> for couchbase_core::searchx::queries::MatchNoneQuery {
    fn from(_: MatchNoneQuery) -> Self {
        couchbase_core::searchx::queries::MatchNoneQuery::new()
    }
}

impl From<GeoDistanceQuery> for couchbase_core::searchx::queries::GeoDistanceQuery {
    fn from(query: GeoDistanceQuery) -> Self {
        couchbase_core::searchx::queries::GeoDistanceQuery::new(
            query.distance,
            query.location.into(),
        )
        .boost(query.boost)
        .field(query.field)
    }
}

impl From<GeoBoundingBoxQuery> for couchbase_core::searchx::queries::GeoBoundingBoxQuery {
    fn from(query: GeoBoundingBoxQuery) -> Self {
        couchbase_core::searchx::queries::GeoBoundingBoxQuery::new(
            query.top_left,
            query.bottom_right,
        )
        .boost(query.boost)
        .field(query.field)
    }
}

impl From<GeoPolygonQuery> for couchbase_core::searchx::queries::GeoPolygonQuery {
    fn from(query: GeoPolygonQuery) -> Self {
        couchbase_core::searchx::queries::GeoPolygonQuery::new(
            query
                .polygon_points
                .into_iter()
                .map(Into::into)
                .collect::<Vec<couchbase_core::searchx::query_options::Location>>(),
        )
        .boost(query.boost)
        .field(query.field)
    }
}

impl From<CustomScoreQuery> for couchbase_core::searchx::queries::CustomScoreQuery {
    fn from(query: CustomScoreQuery) -> Self {
        couchbase_core::searchx::queries::CustomScoreQuery::new((*query.query).into(), query.source)
            .fields(query.fields)
            .params(query.params)
    }
}

impl From<CustomFilterQuery> for couchbase_core::searchx::queries::CustomFilterQuery {
    fn from(query: CustomFilterQuery) -> Self {
        couchbase_core::searchx::queries::CustomFilterQuery::new(
            (*query.query).into(),
            query.source,
        )
        .fields(query.fields)
        .params(query.params)
    }
}

impl From<Query> for couchbase_core::searchx::queries::Query {
    fn from(query: Query) -> Self {
        match query {
            Query::Match(q) => couchbase_core::searchx::queries::Query::Match(q.into()),
            Query::MatchPhrase(q) => couchbase_core::searchx::queries::Query::MatchPhrase(q.into()),
            Query::Regexp(q) => couchbase_core::searchx::queries::Query::Regexp(q.into()),
            Query::QueryString(q) => couchbase_core::searchx::queries::Query::QueryString(q.into()),
            Query::NumericRange(q) => {
                couchbase_core::searchx::queries::Query::NumericRange(q.into())
            }
            Query::DateRange(q) => couchbase_core::searchx::queries::Query::DateRange(q.into()),
            Query::TermRange(q) => couchbase_core::searchx::queries::Query::TermRange(q.into()),
            Query::Conjunction(q) => couchbase_core::searchx::queries::Query::Conjunction(q.into()),
            Query::Disjunction(q) => couchbase_core::searchx::queries::Query::Disjunction(q.into()),
            Query::Boolean(q) => couchbase_core::searchx::queries::Query::Boolean(q.into()),
            Query::Wildcard(q) => couchbase_core::searchx::queries::Query::Wildcard(q.into()),
            Query::DocID(q) => couchbase_core::searchx::queries::Query::DocID(q.into()),
            Query::BooleanField(q) => {
                couchbase_core::searchx::queries::Query::BooleanField(q.into())
            }
            Query::Term(q) => couchbase_core::searchx::queries::Query::Term(q.into()),
            Query::Phrase(q) => couchbase_core::searchx::queries::Query::Phrase(q.into()),
            Query::Prefix(q) => couchbase_core::searchx::queries::Query::Prefix(q.into()),
            Query::MatchAll(q) => couchbase_core::searchx::queries::Query::MatchAll(q.into()),
            Query::MatchNone(q) => couchbase_core::searchx::queries::Query::MatchNone(q.into()),
            Query::GeoDistance(q) => couchbase_core::searchx::queries::Query::GeoDistance(q.into()),
            Query::GeoBoundingBox(q) => {
                couchbase_core::searchx::queries::Query::GeoBoundingBox(q.into())
            }
            Query::GeoPolygon(q) => couchbase_core::searchx::queries::Query::GeoPolygon(q.into()),
            Query::CustomScore(q) => couchbase_core::searchx::queries::Query::CustomScore(q.into()),
            Query::CustomFilter(q) => {
                couchbase_core::searchx::queries::Query::CustomFilter(q.into())
            }
        }
    }
}
