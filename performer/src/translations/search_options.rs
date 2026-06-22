use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::search::search_facet::Facet;
use crate::proto::protocol::sdk::search::{
    search_sort, Highlight, HighlightStyle, SearchFacet, SearchGeoDistanceUnits, SearchOptions,
    SearchScanConsistency, SearchSort, VectorQueryCombination, VectorSearchOptions,
};
use chrono::{DateTime, FixedOffset, Utc};
use couchbase::options::search_options::ScanConsistency::NotBounded;
use couchbase::search::sort::{Sort, SortFieldMode, SortFieldType, SortGeoDistanceUnit};
use prost_types::Timestamp;
use std::time::SystemTime;

impl TryFrom<SearchOptions> for couchbase::options::search_options::SearchOptions {
    type Error = Box<Error>;

    fn try_from(opts: SearchOptions) -> Result<Self> {
        let mut copts = couchbase::options::search_options::SearchOptions::new();

        if let Some(limit) = opts.limit {
            copts = copts.limit(limit);
        }
        if let Some(skip) = opts.skip {
            copts = copts.skip(skip);
        }
        if let Some(explain) = opts.explain {
            copts = copts.explain(explain);
        }
        if let Some(highlight) = opts.highlight {
            copts = copts.highlight(highlight.try_into()?);
        }
        copts = copts.fields(opts.fields);
        if let Some(scan_consistency) = opts.scan_consistency {
            match SearchScanConsistency::try_from(scan_consistency) {
                Ok(SearchScanConsistency::NotBounded) => {
                    copts = copts.scan_consistency(NotBounded);
                }
                Err(_) => {
                    return Err(Error::invalid_argument(
                        "Invalid scan consistency specified",
                    ))
                }
            }
        }
        if let Some(consistent_with) = opts.consistent_with {
            copts = copts.consistent_with(consistent_with.into());
        }
        let sort: Vec<_> = opts
            .sort
            .into_iter()
            .map(|s| s.try_into())
            .collect::<std::result::Result<_, _>>()?;
        copts = copts.sort(sort);

        let facets: std::collections::HashMap<_, _> = opts
            .facets
            .into_iter()
            .map(|(k, v)| v.try_into().map(|facet| (k, facet)))
            .collect::<std::result::Result<_, _>>()?;
        copts = copts.facets(facets);

        if let Some(_timeout) = opts.timeout_millis {
            return Err(Error::unimplemented("timeout is unimplemented"));
        }
        for (k, v) in opts.raw {
            copts = copts
                .add_raw(k, v)
                .map_err(|e| Error::internal(format!("Failed to add raw option: {e}")))?;
        }
        if let Some(include_locations) = opts.include_locations {
            copts = copts.include_locations(include_locations);
        }
        if let Some(_serializer) = opts.serialize {
            return Err(Error::unimplemented("serializer is unimplemented"));
        }
        Ok(copts)
    }
}

impl TryFrom<Highlight> for couchbase::options::search_options::Highlight {
    type Error = Box<Error>;

    fn try_from(proto: Highlight) -> Result<Self> {
        let mut highlight = couchbase::options::search_options::Highlight::new();

        if let Some(style) = proto.style {
            match HighlightStyle::try_from(style) {
                Ok(HighlightStyle::Html) => {
                    highlight =
                        highlight.style(couchbase::options::search_options::HighlightStyle::Html)
                }
                Ok(HighlightStyle::Ansi) => {
                    highlight =
                        highlight.style(couchbase::options::search_options::HighlightStyle::Ansi)
                }
                Err(e) => {
                    return Err(Error::invalid_argument(format!(
                        "Invalid highlight style: {e}"
                    )))
                }
            }
        }
        highlight = highlight.fields(proto.fields);

        Ok(highlight)
    }
}

impl TryFrom<SearchSort> for Sort {
    type Error = Box<Error>;

    fn try_from(proto: SearchSort) -> Result<Self> {
        let sort = proto
            .sort
            .ok_or_else(|| Error::invalid_argument("Search sort is not set"))?;

        match sort {
            search_sort::Sort::Score(score) => {
                let mut sort_score = couchbase::search::sort::SortScore::new();

                if let Some(descending) = score.desc {
                    sort_score = sort_score.descending(descending);
                }
                Ok(Sort::Score(sort_score))
            }
            search_sort::Sort::Id(id) => {
                let mut sort_id = couchbase::search::sort::SortId::new();
                if let Some(descending) = id.desc {
                    sort_id = sort_id.descending(descending);
                }
                Ok(Sort::Id(sort_id))
            }
            search_sort::Sort::Field(field) => {
                let mut sort_field = couchbase::search::sort::SortField::new(field.field);

                if let Some(descending) = field.desc {
                    sort_field = sort_field.descending(descending);
                }
                if let Some(field_type) = field.r#type {
                    sort_field = match field_type.as_str() {
                        "auto" => sort_field.sort_type(SortFieldType::Auto),
                        "string" => sort_field.sort_type(SortFieldType::String),
                        "number" => sort_field.sort_type(SortFieldType::Number),
                        "date" => sort_field.sort_type(SortFieldType::Date),
                        _ => return Err(Error::invalid_argument("Invalid sort type")),
                    }
                }
                if let Some(mode) = field.mode {
                    sort_field = match mode.as_str() {
                        "default" => sort_field.mode(SortFieldMode::Default),
                        "min" => sort_field.mode(SortFieldMode::Min),
                        "max" => sort_field.mode(SortFieldMode::Max),
                        _ => return Err(Error::invalid_argument("Invalid sort mode")),
                    }
                }
                if let Some(missing) = field.missing {
                    sort_field = match missing.as_str() {
                        "last" => {
                            sort_field.missing(couchbase::search::sort::SortFieldMissing::Last)
                        }
                        "first" => {
                            sort_field.missing(couchbase::search::sort::SortFieldMissing::First)
                        }
                        _ => return Err(Error::invalid_argument("Invalid sort missing value")),
                    }
                }

                Ok(Sort::Field(sort_field))
            }
            search_sort::Sort::GeoDistance(geo) => {
                let location = geo.location.ok_or_else(|| {
                    Error::invalid_argument("SearchSortGeoDistance location is not set")
                })?;

                let location = couchbase::search::location::Location::new(
                    location.lat as f64,
                    location.lon as f64,
                );

                let mut sort_geo_distance =
                    couchbase::search::sort::SortGeoDistance::new(geo.field, location);
                if let Some(descending) = geo.desc {
                    sort_geo_distance = sort_geo_distance.descending(descending);
                }
                if let Some(unit) = geo.unit {
                    sort_geo_distance = match SearchGeoDistanceUnits::try_from(unit) {
                        Ok(SearchGeoDistanceUnits::Meters) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Meters)
                        }
                        Ok(SearchGeoDistanceUnits::Miles) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Miles)
                        }
                        Ok(SearchGeoDistanceUnits::Centimeters) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Centimeters)
                        }
                        Ok(SearchGeoDistanceUnits::Millimeters) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Millimeters)
                        }
                        Ok(SearchGeoDistanceUnits::NauticalMiles) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::NauticalMiles)
                        }
                        Ok(SearchGeoDistanceUnits::Kilometers) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Kilometers)
                        }
                        Ok(SearchGeoDistanceUnits::Feet) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Feet)
                        }
                        Ok(SearchGeoDistanceUnits::Yards) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Yards)
                        }
                        Ok(SearchGeoDistanceUnits::Inches) => {
                            sort_geo_distance.unit(SortGeoDistanceUnit::Inches)
                        }
                        Err(_) => return Err(Error::invalid_argument("Invalid geo distance unit")),
                    };
                }
                Ok(Sort::GeoDistance(sort_geo_distance))
            }
            search_sort::Sort::Raw(_) => Err(Error::unimplemented("Raw sort is not implemented")),
        }
    }
}

impl TryFrom<SearchFacet> for couchbase::search::facets::Facet {
    type Error = Box<Error>;

    fn try_from(proto: SearchFacet) -> Result<Self> {
        let facet = proto
            .facet
            .ok_or_else(|| Error::invalid_argument("Search facet is not set"))?;

        match facet {
            Facet::Term(term) => {
                let mut term_facet = couchbase::search::facets::TermFacet::new(term.field);
                if let Some(size) = term.size {
                    term_facet = term_facet.size(size as u64);
                }
                Ok(couchbase::search::facets::Facet::Term(term_facet))
            }
            Facet::NumericRange(numeric) => {
                let mut numeric_range_facet =
                    couchbase::search::facets::NumericRangeFacet::new(numeric.field);
                if let Some(size) = numeric.size {
                    numeric_range_facet = numeric_range_facet.size(size as u64);
                }
                for range in numeric.numeric_ranges {
                    let mut numeric_range =
                        couchbase::search::facets::NumericRange::new(range.name);
                    if let Some(min) = range.min {
                        numeric_range = numeric_range.min(min as f64);
                    }
                    if let Some(max) = range.max {
                        numeric_range = numeric_range.max(max as f64);
                    }
                    numeric_range_facet = numeric_range_facet.add_numeric_range(numeric_range);
                }
                Ok(couchbase::search::facets::Facet::NumericRange(
                    numeric_range_facet,
                ))
            }
            Facet::DateRange(date) => {
                let mut date_range_facet =
                    couchbase::search::facets::DateRangeFacet::new(date.field);
                if let Some(size) = date.size {
                    date_range_facet = date_range_facet.size(size as u64);
                }
                for range in date.date_ranges {
                    let mut date_range = couchbase::search::facets::DateRange::new(range.name);

                    if let Some(start) = range.start {
                        let date_time = timestamp_to_fixedoffset(start)?;
                        date_range = date_range.start(date_time);
                    }
                    if let Some(end) = range.end {
                        let date_time = timestamp_to_fixedoffset(end)?;
                        date_range = date_range.end(date_time);
                    }
                    date_range_facet = date_range_facet.add_date_range(date_range);
                }
                Ok(couchbase::search::facets::Facet::DateRange(
                    date_range_facet,
                ))
            }
        }
    }
}

impl TryFrom<VectorSearchOptions> for couchbase::search::vector::VectorSearchOptions {
    type Error = Box<Error>;

    fn try_from(proto: VectorSearchOptions) -> Result<Self> {
        let mut vector_search_options = couchbase::search::vector::VectorSearchOptions::new();

        if let Some(combination) = proto.vector_query_combination {
            vector_search_options = match VectorQueryCombination::try_from(combination) {
                Ok(VectorQueryCombination::And) => vector_search_options
                    .query_combination(couchbase::search::vector::VectorQueryCombination::And),
                Ok(VectorQueryCombination::Or) => vector_search_options
                    .query_combination(couchbase::search::vector::VectorQueryCombination::Or),
                Err(_) => return Err(Error::invalid_argument("Invalid vector query combination")),
            }
        }

        Ok(vector_search_options)
    }
}

fn timestamp_to_fixedoffset(ts: Timestamp) -> Result<DateTime<FixedOffset>> {
    let sys_time = SystemTime::try_from(ts)
        .map_err(|_| Error::invalid_argument("Invalid date range provided"))?;
    let dt_utc: DateTime<Utc> = sys_time.into();
    Ok(dt_utc.into())
}
