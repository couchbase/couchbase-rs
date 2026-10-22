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
use crate::searchx::query_options::Location;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MatchOperator {
    Or,
    And,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct MatchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzziness: Option<u64>,
    #[serde(rename = "match")]
    pub match_input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<MatchOperator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u64>,
}

impl MatchQuery {
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

    pub fn analyzer(mut self, analyzer: impl Into<Option<String>>) -> Self {
        self.analyzer = analyzer.into();
        self
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }

    pub fn fuzziness(mut self, fuzziness: impl Into<Option<u64>>) -> Self {
        self.fuzziness = fuzziness.into();
        self
    }

    pub fn operator(mut self, operator: impl Into<Option<MatchOperator>>) -> Self {
        self.operator = operator.into();
        self
    }

    pub fn prefix_length(mut self, prefix_length: impl Into<Option<u64>>) -> Self {
        self.prefix_length = prefix_length.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct MatchPhraseQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub match_phrase: String,
}

impl MatchPhraseQuery {
    pub fn new(match_phrase: impl Into<String>) -> Self {
        Self {
            analyzer: None,
            boost: None,
            field: None,
            match_phrase: match_phrase.into(),
        }
    }

    pub fn analyzer(mut self, analyzer: impl Into<Option<String>>) -> Self {
        self.analyzer = analyzer.into();
        self
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct RegexpQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub regexp: String,
}

impl RegexpQuery {
    pub fn new(regexp: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            regexp: regexp.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct QueryStringQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    pub query: String,
}

impl QueryStringQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            boost: None,
            query: query.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct NumericRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_min: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_max: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f32>,
}

impl NumericRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }

    pub fn inclusive_min(mut self, inclusive_min: impl Into<Option<bool>>) -> Self {
        self.inclusive_min = inclusive_min.into();
        self
    }

    pub fn inclusive_max(mut self, inclusive_max: impl Into<Option<bool>>) -> Self {
        self.inclusive_max = inclusive_max.into();
        self
    }

    pub fn min(mut self, min: impl Into<Option<f32>>) -> Self {
        self.min = min.into();
        self
    }

    pub fn max(mut self, max: impl Into<Option<f32>>) -> Self {
        self.max = max.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct DateRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datetime_parser: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_start: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_end: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
}

impl DateRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }

    pub fn datetime_parser(mut self, datetime_parser: impl Into<Option<String>>) -> Self {
        self.datetime_parser = datetime_parser.into();
        self
    }

    pub fn end(mut self, end: impl Into<Option<String>>) -> Self {
        self.end = end.into();
        self
    }

    pub fn inclusive_start(mut self, inclusive_start: impl Into<Option<bool>>) -> Self {
        self.inclusive_start = inclusive_start.into();
        self
    }

    pub fn inclusive_end(mut self, inclusive_end: impl Into<Option<bool>>) -> Self {
        self.inclusive_end = inclusive_end.into();
        self
    }

    pub fn start(mut self, start: impl Into<Option<String>>) -> Self {
        self.start = start.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct TermRangeQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_min: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inclusive_max: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
}

impl TermRangeQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }

    pub fn inclusive_min(mut self, inclusive_min: impl Into<Option<bool>>) -> Self {
        self.inclusive_min = inclusive_min.into();
        self
    }

    pub fn inclusive_max(mut self, inclusive_max: impl Into<Option<bool>>) -> Self {
        self.inclusive_max = inclusive_max.into();
        self
    }

    pub fn max(mut self, max: impl Into<Option<String>>) -> Self {
        self.max = max.into();
        self
    }

    pub fn min(mut self, min: impl Into<Option<String>>) -> Self {
        self.min = min.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct ConjunctionQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    pub conjuncts: Vec<Query>,
}

impl ConjunctionQuery {
    pub fn new(conjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            conjuncts,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct DisjunctionQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
    pub disjuncts: Vec<Query>,
}

impl DisjunctionQuery {
    pub fn new(disjuncts: Vec<Query>) -> Self {
        Self {
            boost: None,
            disjuncts,
            min: None,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn min(mut self, min: impl Into<Option<u32>>) -> Self {
        self.min = min.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct BooleanQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must: Option<ConjunctionQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_not: Option<DisjunctionQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should: Option<DisjunctionQuery>,
}

impl BooleanQuery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn must(mut self, must: impl Into<Option<ConjunctionQuery>>) -> Self {
        self.must = must.into();
        self
    }

    pub fn must_not(mut self, must_not: impl Into<Option<DisjunctionQuery>>) -> Self {
        self.must_not = must_not.into();
        self
    }

    pub fn should(mut self, should: impl Into<Option<DisjunctionQuery>>) -> Self {
        self.should = should.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct WildcardQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub wildcard: String,
}

impl WildcardQuery {
    pub fn new(wildcard: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            wildcard: wildcard.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct DocIDQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    pub ids: Vec<String>,
}

impl DocIDQuery {
    pub fn new(ids: Vec<String>) -> Self {
        Self { boost: None, ids }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct BooleanFieldQuery {
    #[serde(rename = "bool")]
    pub bool_value: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

impl BooleanFieldQuery {
    pub fn new(bool_value: bool) -> Self {
        Self {
            bool_value,
            boost: None,
            field: None,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct TermQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuzziness: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_length: Option<u32>,
    pub term: String,
}

impl TermQuery {
    pub fn new(term: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            fuzziness: None,
            prefix_length: None,
            term: term.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }

    pub fn fuzziness(mut self, fuzziness: impl Into<Option<u32>>) -> Self {
        self.fuzziness = fuzziness.into();
        self
    }

    pub fn prefix_length(mut self, prefix_length: impl Into<Option<u32>>) -> Self {
        self.prefix_length = prefix_length.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct PhraseQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub terms: Vec<String>,
}

impl PhraseQuery {
    pub fn new(terms: Vec<String>) -> Self {
        Self {
            boost: None,
            field: None,
            terms,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct PrefixQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub prefix: String,
}

impl PrefixQuery {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            boost: None,
            field: None,
            prefix: prefix.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchAllQuery {}

impl MatchAllQuery {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Serialize for MatchAllQuery {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("match_all", &serde_json::Value::Null)?;
        map.end()
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchNoneQuery {}

impl MatchNoneQuery {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Serialize for MatchNoneQuery {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("match_none", &serde_json::Value::Null)?;
        map.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct GeoDistanceQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub location: Location,
    pub distance: String,
}

impl GeoDistanceQuery {
    pub fn new(distance: impl Into<String>, location: Location) -> Self {
        Self {
            distance: distance.into(),
            boost: None,
            field: None,
            location,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct GeoBoundingBoxQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub top_left: Location,
    pub bottom_right: Location,
}

impl GeoBoundingBoxQuery {
    pub fn new(top_left: impl Into<Location>, bottom_right: impl Into<Location>) -> Self {
        Self {
            bottom_right: bottom_right.into(),
            boost: None,
            field: None,
            top_left: top_left.into(),
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct GeoPolygonQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub polygon_points: Vec<Location>,
}

impl GeoPolygonQuery {
    pub fn new(polygon_points: Vec<Location>) -> Self {
        Self {
            boost: None,
            field: None,
            polygon_points,
        }
    }

    pub fn boost(mut self, boost: impl Into<Option<f32>>) -> Self {
        self.boost = boost.into();
        self
    }

    pub fn field(mut self, field: impl Into<Option<String>>) -> Self {
        self.field = field.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct CustomScoreQuery {
    pub query: Box<Query>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Value>>,
}

impl CustomScoreQuery {
    pub fn new(query: Query, source: impl Into<String>) -> Self {
        Self {
            query: Box::new(query),
            source: source.into(),
            fields: None,
            params: None,
        }
    }

    pub fn fields(mut self, fields: impl Into<Option<Vec<String>>>) -> Self {
        self.fields = fields.into();
        self
    }

    pub fn params(mut self, params: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.params = params.into();
        self
    }
}

fn serialize_custom_score<S>(query: &CustomScoreQuery, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry("custom_score", query)?;
    map.end()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct CustomFilterQuery {
    pub query: Box<Query>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Value>>,
}

impl CustomFilterQuery {
    pub fn new(query: Query, source: impl Into<String>) -> Self {
        Self {
            query: Box::new(query),
            source: source.into(),
            fields: None,
            params: None,
        }
    }

    pub fn fields(mut self, fields: impl Into<Option<Vec<String>>>) -> Self {
        self.fields = fields.into();
        self
    }

    pub fn params(mut self, params: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.params = params.into();
        self
    }
}

fn serialize_custom_filter<S>(query: &CustomFilterQuery, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry("custom_filter", query)?;
    map.end()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Query {
    Match(MatchQuery),
    MatchPhrase(MatchPhraseQuery),
    Regexp(RegexpQuery),
    QueryString(QueryStringQuery),
    NumericRange(NumericRangeQuery),
    DateRange(DateRangeQuery),
    TermRange(TermRangeQuery),
    Conjunction(ConjunctionQuery),
    Disjunction(DisjunctionQuery),
    Boolean(BooleanQuery),
    Wildcard(WildcardQuery),
    DocID(DocIDQuery),
    BooleanField(BooleanFieldQuery),
    Term(TermQuery),
    Phrase(PhraseQuery),
    Prefix(PrefixQuery),
    MatchAll(MatchAllQuery),
    MatchNone(MatchNoneQuery),
    GeoDistance(GeoDistanceQuery),
    GeoBoundingBox(GeoBoundingBoxQuery),
    GeoPolygon(GeoPolygonQuery),
    #[serde(serialize_with = "serialize_custom_score")]
    CustomScore(CustomScoreQuery),
    #[serde(serialize_with = "serialize_custom_filter")]
    CustomFilter(CustomFilterQuery),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn match_query() -> Query {
        Query::Match(MatchQuery::new("hotel").field("type".to_string()))
    }

    #[test]
    fn custom_score_query_serializes_all_fields() {
        let query = Query::CustomScore(
            CustomScoreQuery::new(
                match_query(),
                "function f(doc, params) { return doc.score; }",
            )
            .fields(vec!["price".to_string()])
            .params(HashMap::from([("scale".to_string(), json!(2))])),
        );

        assert_eq!(
            serde_json::to_value(&query).unwrap(),
            json!({
                "custom_score": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function f(doc, params) { return doc.score; }",
                    "fields": ["price"],
                    "params": {"scale": 2}
                }
            })
        );
    }

    #[test]
    fn custom_filter_query_omits_unset_fields_and_params() {
        let query = Query::CustomFilter(CustomFilterQuery::new(
            match_query(),
            "function f(doc, params) { return true; }",
        ));

        assert_eq!(
            serde_json::to_value(&query).unwrap(),
            json!({
                "custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function f(doc, params) { return true; }"
                }
            })
        );
    }

    #[test]
    fn custom_queries_serialize_when_nested() {
        let query = Query::Conjunction(ConjunctionQuery::new(vec![Query::CustomScore(
            CustomScoreQuery::new(
                Query::CustomFilter(CustomFilterQuery::new(match_query(), "filter")),
                "score",
            ),
        )]));

        assert_eq!(
            serde_json::to_value(&query).unwrap(),
            json!({
                "conjuncts": [{
                    "custom_score": {
                        "query": {
                            "custom_filter": {
                                "query": {"match": "hotel", "field": "type"},
                                "source": "filter"
                            }
                        },
                        "source": "score"
                    }
                }]
            })
        );
    }
}
