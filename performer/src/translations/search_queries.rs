use crate::errors::error::Error;
use crate::proto::protocol::sdk::search;
use crate::proto::protocol::sdk::search::SearchQuery;
use couchbase::search::queries::{
    BooleanFieldQuery, BooleanQuery, ConjunctionQuery, DateRangeQuery, DisjunctionQuery,
    DocIDQuery, GeoBoundingBoxQuery, GeoDistanceQuery, MatchAllQuery, MatchNoneQuery,
    MatchOperator, MatchPhraseQuery, MatchQuery, NumericRangeQuery, PhraseQuery, PrefixQuery,
    Query, QueryStringQuery, RegexpQuery, TermQuery, TermRangeQuery, WildcardQuery,
};
use couchbase::search::request::SearchRequest;
use couchbase::search::vector::{VectorQuery, VectorSearch};

impl TryFrom<crate::proto::protocol::sdk::search::SearchRequest> for SearchRequest {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::SearchRequest,
    ) -> crate::errors::error::Result<Self> {
        let mut request: Option<SearchRequest> = None;

        if let Some(q) = proto.search_query {
            let q = Query::try_from(q)?;
            request = Some(SearchRequest::with_search_query(q));
        }

        if let Some(v) = proto.vector_search {
            let v = VectorSearch::try_from(v)?;
            match request {
                Some(mut req) => {
                    req = req.vector_search(v).map_err(Error::sdk)?;
                    return Ok(req);
                }
                None => {
                    request = Some(SearchRequest::with_vector_search(v));
                }
            }
        }

        request.ok_or_else(|| Error::invalid_argument("No search query or vector search provided"))
    }
}

impl TryFrom<SearchQuery> for Query {
    type Error = Box<Error>;

    fn try_from(proto: SearchQuery) -> crate::errors::error::Result<Self> {
        match proto.query.unwrap() {
            crate::proto::protocol::sdk::search::search_query::Query::Match(query) => {
                let match_query = MatchQuery::try_from(query)?;
                Ok(Query::Match(match_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::MatchPhrase(query) => {
                let match_phrase_query = MatchPhraseQuery::try_from(query)?;
                Ok(Query::MatchPhrase(match_phrase_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Regexp(query) => {
                let regexp_query = RegexpQuery::try_from(query)?;
                Ok(Query::Regexp(regexp_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::QueryString(query) => {
                let query_string_query = QueryStringQuery::try_from(query)?;
                Ok(Query::QueryString(query_string_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Wildcard(query) => {
                let wildcard_query = WildcardQuery::try_from(query)?;
                Ok(Query::Wildcard(wildcard_query))
            }
            search::search_query::Query::DocId(query) => {
                let doc_id_query = DocIDQuery::try_from(query)?;
                Ok(Query::DocID(doc_id_query))
            }
            search::search_query::Query::SearchBooleanField(query) => {
                let boolean_field_query = BooleanFieldQuery::try_from(query)?;
                Ok(Query::BooleanField(boolean_field_query))
            }
            search::search_query::Query::DateRange(query) => {
                let date_range_query = DateRangeQuery::try_from(query)?;
                Ok(Query::DateRange(date_range_query))
            }
            search::search_query::Query::NumericRange(query) => {
                let numeric_range_query = NumericRangeQuery::try_from(query)?;
                Ok(Query::NumericRange(numeric_range_query))
            }
            search::search_query::Query::TermRange(query) => {
                let term_range_query = TermRangeQuery::try_from(query)?;
                Ok(Query::TermRange(term_range_query))
            }
            search::search_query::Query::GeoDistance(query) => {
                let geo_distance_query = GeoDistanceQuery::try_from(query)?;
                Ok(Query::GeoDistance(geo_distance_query))
            }
            search::search_query::Query::GeoBoundingBox(query) => {
                let geo_bounding_box_query = GeoBoundingBoxQuery::try_from(query)?;
                Ok(Query::GeoBoundingBox(geo_bounding_box_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Conjunction(query) => {
                let conjunction_query = ConjunctionQuery::try_from(query)?;
                Ok(Query::Conjunction(conjunction_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Disjunction(query) => {
                let disjunction_query = DisjunctionQuery::try_from(query)?;
                Ok(Query::Disjunction(disjunction_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Boolean(query) => {
                let boolean_query = BooleanQuery::try_from(query)?;
                Ok(Query::Boolean(boolean_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Term(query) => {
                let term_query = TermQuery::try_from(query)?;
                Ok(Query::Term(term_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Prefix(query) => {
                let prefix_query = PrefixQuery::try_from(query)?;
                Ok(Query::Prefix(prefix_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::Phrase(query) => {
                let phrase_query = PhraseQuery::try_from(query)?;
                Ok(Query::Phrase(phrase_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::MatchAll(query) => {
                let match_all_query = MatchAllQuery::try_from(query)?;
                Ok(Query::MatchAll(match_all_query))
            }
            crate::proto::protocol::sdk::search::search_query::Query::MatchNone(query) => {
                let match_none_query = MatchNoneQuery::try_from(query)?;
                Ok(Query::MatchNone(match_none_query))
            }
        }
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::MatchQuery> for MatchQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::MatchQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut match_query = MatchQuery::new(proto.r#match);
        if let Some(field) = proto.field {
            match_query = match_query.field(field);
        }
        if let Some(analyzer) = proto.analyzer {
            match_query = match_query.analyzer(analyzer);
        }
        if let Some(prefix_length) = proto.prefix_length {
            match_query = match_query.prefix_length(prefix_length as u64);
        }
        if let Some(fuzziness) = proto.fuzziness {
            match_query = match_query.fuzziness(fuzziness as u64);
        }
        if let Some(boost) = proto.boost {
            match_query = match_query.boost(boost);
        }
        if let Some(operator) = proto.operator {
            match_query =
                match crate::proto::protocol::sdk::search::MatchOperator::try_from(operator) {
                    Ok(
                        crate::proto::protocol::sdk::search::MatchOperator::SearchMatchOperatorOr,
                    ) => match_query.operator(MatchOperator::Or),
                    Ok(
                        crate::proto::protocol::sdk::search::MatchOperator::SearchMatchOperatorAnd,
                    ) => match_query.operator(MatchOperator::And),
                    _ => return Err(Error::invalid_argument("Invalid match operator")),
                };
        }
        Ok(match_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::MatchPhraseQuery> for MatchPhraseQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::MatchPhraseQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut match_phrase_query = MatchPhraseQuery::new(proto.match_phrase);
        if let Some(field) = proto.field {
            match_phrase_query = match_phrase_query.field(field);
        }
        if let Some(analyzer) = proto.analyzer {
            match_phrase_query = match_phrase_query.analyzer(analyzer);
        }
        if let Some(boost) = proto.boost {
            match_phrase_query = match_phrase_query.boost(boost);
        }
        Ok(match_phrase_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::RegexpQuery> for RegexpQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::RegexpQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut regexp_query = RegexpQuery::new(proto.regexp);
        if let Some(field) = proto.field {
            regexp_query = regexp_query.field(field);
        }
        if let Some(boost) = proto.boost {
            regexp_query = regexp_query.boost(boost);
        }
        Ok(regexp_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::QueryStringQuery> for QueryStringQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::QueryStringQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut query_string_query = QueryStringQuery::new(proto.query);

        if let Some(boost) = proto.boost {
            query_string_query = query_string_query.boost(boost);
        }

        Ok(query_string_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::WildcardQuery> for WildcardQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::WildcardQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut wildcard_query = WildcardQuery::new(proto.wildcard);
        if let Some(field) = proto.field {
            wildcard_query = wildcard_query.field(field);
        }
        if let Some(boost) = proto.boost {
            wildcard_query = wildcard_query.boost(boost);
        }
        Ok(wildcard_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::DocIdQuery> for DocIDQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::DocIdQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut doc_id_query = DocIDQuery::new(proto.ids);
        if let Some(boost) = proto.boost {
            doc_id_query = doc_id_query.boost(boost);
        }
        Ok(doc_id_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::BooleanFieldQuery> for BooleanFieldQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::BooleanFieldQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut boolean_field_query = BooleanFieldQuery::new(proto.bool);
        if let Some(field) = proto.field {
            boolean_field_query = boolean_field_query.field(field);
        }
        if let Some(boost) = proto.boost {
            boolean_field_query = boolean_field_query.boost(boost);
        }
        Ok(boolean_field_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::DateRangeQuery> for DateRangeQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::DateRangeQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut date_range_query = DateRangeQuery::new();

        date_range_query = match (proto.start, proto.inclusive_start) {
            (Some(start), Some(inclusive_start)) => {
                date_range_query.inclusive_start(start, inclusive_start)
            }
            (Some(start), None) => date_range_query.start(start),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive start provided without start",
                ))
            }
            (None, None) => date_range_query,
        };
        date_range_query = match (proto.end, proto.inclusive_end) {
            (Some(end), Some(inclusive_end)) => date_range_query.inclusive_end(end, inclusive_end),
            (Some(end), None) => date_range_query.end(end),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive end provided without end",
                ))
            }
            (None, None) => date_range_query,
        };
        if let Some(datetime_parser) = proto.datetime_parser {
            date_range_query = date_range_query.datetime_parser(datetime_parser);
        }
        if let Some(field) = proto.field {
            date_range_query = date_range_query.field(field);
        }
        if let Some(boost) = proto.boost {
            date_range_query = date_range_query.boost(boost);
        }
        Ok(date_range_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::NumericRangeQuery> for NumericRangeQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::NumericRangeQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut numeric_range_query = NumericRangeQuery::new();

        numeric_range_query = match (proto.min, proto.inclusive_min) {
            (Some(min), Some(inclusive_min)) => {
                numeric_range_query.inclusive_min(min, inclusive_min)
            }
            (Some(min), None) => numeric_range_query.min(min),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive min provided without min",
                ))
            }
            (None, None) => numeric_range_query,
        };
        numeric_range_query = match (proto.max, proto.inclusive_max) {
            (Some(max), Some(inclusive_max)) => {
                numeric_range_query.inclusive_max(max, inclusive_max)
            }
            (Some(max), None) => numeric_range_query.max(max),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive max provided without max",
                ))
            }
            (None, None) => numeric_range_query,
        };
        if let Some(field) = proto.field {
            numeric_range_query = numeric_range_query.field(field);
        }
        if let Some(boost) = proto.boost {
            numeric_range_query = numeric_range_query.boost(boost);
        }
        Ok(numeric_range_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::TermRangeQuery> for TermRangeQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::TermRangeQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut term_range_query = TermRangeQuery::new();

        term_range_query = match (proto.min, proto.inclusive_min) {
            (Some(min), Some(inclusive_min)) => term_range_query.inclusive_min(min, inclusive_min),
            (Some(min), None) => term_range_query.min(min),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive min provided without min",
                ))
            }
            (None, None) => term_range_query,
        };
        term_range_query = match (proto.max, proto.inclusive_max) {
            (Some(max), Some(inclusive_max)) => term_range_query.inclusive_max(max, inclusive_max),
            (Some(max), None) => term_range_query.max(max),
            (None, Some(_)) => {
                return Err(Error::invalid_argument(
                    "Inclusive max provided without max",
                ))
            }
            (None, None) => term_range_query,
        };
        if let Some(field) = proto.field {
            term_range_query = term_range_query.field(field);
        }
        if let Some(boost) = proto.boost {
            term_range_query = term_range_query.boost(boost);
        }
        Ok(term_range_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::GeoDistanceQuery> for GeoDistanceQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::GeoDistanceQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut geo_distance_query =
            GeoDistanceQuery::new(proto.distance, proto.location.unwrap().into());
        if let Some(field) = proto.field {
            geo_distance_query = geo_distance_query.field(field);
        }
        if let Some(boost) = proto.boost {
            geo_distance_query = geo_distance_query.boost(boost);
        }
        Ok(geo_distance_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::GeoBoundingBoxQuery> for GeoBoundingBoxQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::GeoBoundingBoxQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut geo_bounding_box_query = GeoBoundingBoxQuery::new(
            proto.bottom_right.unwrap().into(),
            proto.top_left.unwrap().into(),
        );
        if let Some(field) = proto.field {
            geo_bounding_box_query = geo_bounding_box_query.field(field);
        }
        if let Some(boost) = proto.boost {
            geo_bounding_box_query = geo_bounding_box_query.boost(boost);
        }
        Ok(geo_bounding_box_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::ConjunctionQuery> for ConjunctionQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::ConjunctionQuery,
    ) -> crate::errors::error::Result<Self> {
        let conjuncts: Vec<_> = proto
            .conjuncts
            .into_iter()
            .map(|s| s.try_into())
            .collect::<std::result::Result<_, _>>()?;

        let mut conjunction_query = ConjunctionQuery::new(conjuncts);

        if let Some(boost) = proto.boost {
            conjunction_query = conjunction_query.boost(boost);
        }
        Ok(conjunction_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::DisjunctionQuery> for DisjunctionQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::DisjunctionQuery,
    ) -> crate::errors::error::Result<Self> {
        let disjuncts: Vec<_> = proto
            .disjuncts
            .into_iter()
            .map(|s| s.try_into())
            .collect::<std::result::Result<_, _>>()?;

        let mut disjunction_query = DisjunctionQuery::new(disjuncts);

        if let Some(min) = proto.min {
            disjunction_query = disjunction_query.min(min);
        }
        if let Some(boost) = proto.boost {
            disjunction_query = disjunction_query.boost(boost);
        }
        Ok(disjunction_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::BooleanQuery> for BooleanQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::BooleanQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut boolean_query = BooleanQuery::new();

        if !proto.must.is_empty() {
            let must_queries: Vec<Query> = proto
                .must
                .into_iter()
                .map(|q| q.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let conjunction = ConjunctionQuery::new(must_queries);
            boolean_query = boolean_query.must(conjunction);
        }
        if !proto.should.is_empty() {
            let should_queries: Vec<Query> = proto
                .should
                .into_iter()
                .map(|q| q.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut disjunction = DisjunctionQuery::new(should_queries);
            if let Some(should_min) = proto.should_min {
                disjunction = disjunction.min(should_min);
            }
            boolean_query = boolean_query.should(disjunction);
        }
        if !proto.must_not.is_empty() {
            let must_not_queries: Vec<Query> = proto
                .must_not
                .into_iter()
                .map(|q| q.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let disjunction = DisjunctionQuery::new(must_not_queries);
            boolean_query = boolean_query.must_not(disjunction);
        }
        if let Some(boost) = proto.boost {
            boolean_query = boolean_query.boost(boost);
        }
        Ok(boolean_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::TermQuery> for TermQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::TermQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut term_query = TermQuery::new(proto.term);
        if let Some(field) = proto.field {
            term_query = term_query.field(field);
        }
        if let Some(fuzziness) = proto.fuzziness {
            term_query = term_query.fuzziness(fuzziness);
        }
        if let Some(prefix_length) = proto.prefix_length {
            term_query = term_query.prefix_length(prefix_length);
        }
        if let Some(boost) = proto.boost {
            term_query = term_query.boost(boost);
        }
        Ok(term_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::PrefixQuery> for PrefixQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::PrefixQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut phrase_query = PrefixQuery::new(proto.prefix);

        if let Some(boost) = proto.boost {
            phrase_query = phrase_query.boost(boost);
        }
        if let Some(field) = proto.field {
            phrase_query = phrase_query.field(field);
        }
        Ok(phrase_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::PhraseQuery> for PhraseQuery {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::PhraseQuery,
    ) -> crate::errors::error::Result<Self> {
        let mut phrase_query = PhraseQuery::new(proto.terms);
        if let Some(field) = proto.field {
            phrase_query = phrase_query.field(field);
        }
        if let Some(boost) = proto.boost {
            phrase_query = phrase_query.boost(boost);
        }
        Ok(phrase_query)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::MatchAllQuery> for MatchAllQuery {
    type Error = Box<Error>;

    fn try_from(
        _proto: crate::proto::protocol::sdk::search::MatchAllQuery,
    ) -> crate::errors::error::Result<Self> {
        Ok(MatchAllQuery::new())
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::MatchNoneQuery> for MatchNoneQuery {
    type Error = Box<Error>;

    fn try_from(
        _proto: crate::proto::protocol::sdk::search::MatchNoneQuery,
    ) -> crate::errors::error::Result<Self> {
        Ok(MatchNoneQuery::new())
    }
}

impl From<crate::proto::protocol::sdk::search::Location> for couchbase::search::location::Location {
    fn from(proto: crate::proto::protocol::sdk::search::Location) -> Self {
        couchbase::search::location::Location::new(proto.lat as f64, proto.lon as f64)
    }
}

impl TryFrom<crate::proto::protocol::sdk::search::VectorSearch> for VectorSearch {
    type Error = Box<Error>;

    fn try_from(
        proto: crate::proto::protocol::sdk::search::VectorSearch,
    ) -> crate::errors::error::Result<Self> {
        let queries: Vec<_> = proto
            .vector_query
            .into_iter()
            .map(|q| {
                let mut vector_query = if let Some(base64) = q.base64_vector_query {
                    VectorQuery::with_base64_vector(q.vector_field_name, base64)
                } else {
                    VectorQuery::with_vector(q.vector_field_name, q.vector_query)
                };

                if let Some(opts) = q.options {
                    if let Some(num_candidates) = opts.num_candidates {
                        vector_query = vector_query.num_candidates(num_candidates as u32);
                    }
                    if let Some(boost) = opts.boost {
                        vector_query = vector_query.boost(boost);
                    }
                    if let Some(prefilter) = opts.prefilter {
                        vector_query = vector_query.prefilter(prefilter.try_into()?);
                    }
                }
                Ok(vector_query)
            })
            .collect::<crate::errors::error::Result<_>>()?;

        let opts = proto.options.map(|opts| opts.try_into()).transpose()?;
        Ok(VectorSearch::new(queries, opts))
    }
}
