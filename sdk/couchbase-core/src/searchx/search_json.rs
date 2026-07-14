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

use crate::searchx::error;
use crate::searchx::search_result::{
    DateRangeFacetResult, FacetResult, HitLocation, NumericRangeFacetResult, ResultHit,
    TermFacetResult,
};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub(crate) struct SearchMetadataStatus {
    #[serde(default)]
    pub(crate) errors: HashMap<String, String>,
    pub(crate) failed: u64,
    pub(crate) successful: u64,
    pub(crate) total: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchMetaData {
    pub(crate) status: SearchMetadataStatus,
    pub(crate) total_hits: u64,
    pub(crate) max_score: f64,
    pub(crate) took: u64,
    #[serde(default)]
    pub(crate) facets: Option<HashMap<String, Facet>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TermFacet {
    pub(crate) term: String,
    pub(crate) count: i64,
}

impl From<TermFacet> for TermFacetResult {
    fn from(value: TermFacet) -> TermFacetResult {
        TermFacetResult {
            term: value.term,
            count: value.count,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct NumericFacet {
    pub(crate) name: String,
    pub(crate) min: Option<f64>,
    pub(crate) max: Option<f64>,
    pub(crate) count: i64,
}

impl From<NumericFacet> for NumericRangeFacetResult {
    fn from(value: NumericFacet) -> NumericRangeFacetResult {
        NumericRangeFacetResult {
            name: value.name,
            min: value.min.unwrap_or_default(),
            max: value.max.unwrap_or_default(),
            count: value.count,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct DateFacet {
    pub(crate) name: String,
    pub(crate) start: String,
    pub(crate) end: String,
    pub(crate) count: i64,
}

impl TryFrom<DateFacet> for DateRangeFacetResult {
    type Error = error::Error;

    fn try_from(value: DateFacet) -> Result<DateRangeFacetResult, Self::Error> {
        Ok(DateRangeFacetResult {
            name: value.name,
            start: DateTime::parse_from_rfc3339(&value.start).map_err(|e| {
                error::Error::new_message_error(format!("failed to parse date: {}", e), None)
            })?,
            end: DateTime::parse_from_rfc3339(&value.end).map_err(|e| {
                error::Error::new_message_error(format!("failed to parse date: {}", e), None)
            })?,
            count: value.count,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Facet {
    pub(crate) field: String,
    pub(crate) total: i64,
    pub(crate) missing: i64,
    pub(crate) other: i64,
    pub(crate) terms: Option<Vec<TermFacet>>,
    pub(crate) numeric_ranges: Option<Vec<NumericFacet>>,
    pub(crate) date_ranges: Option<Vec<DateFacet>>,
}

impl TryFrom<Facet> for FacetResult {
    type Error = error::Error;

    fn try_from(value: Facet) -> Result<FacetResult, Self::Error> {
        let date_ranges = if let Some(date_ranges) = value.date_ranges {
            Some(
                date_ranges
                    .into_iter()
                    .map(DateRangeFacetResult::try_from)
                    .collect::<Result<Vec<DateRangeFacetResult>, Self::Error>>()?,
            )
        } else {
            None
        };

        Ok(FacetResult {
            field: value.field,
            total: value.total,
            missing: value.missing,
            other: value.other,
            terms: value
                .terms
                .map(|terms| terms.into_iter().map(|term| term.into()).collect()),
            numeric_ranges: value.numeric_ranges.map(|numeric_ranges| {
                numeric_ranges
                    .into_iter()
                    .map(|numeric_range| numeric_range.into())
                    .collect()
            }),
            date_ranges,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RowLocation {
    #[serde(rename = "pos")]
    pub(crate) position: u32,
    pub(crate) start: u32,
    pub(crate) end: u32,
    #[serde(default)]
    pub(crate) array_positions: Option<Vec<u32>>,
}

pub(crate) type RowLocations = HashMap<String, HashMap<String, Vec<RowLocation>>>;

#[derive(Debug, Deserialize)]
pub(crate) struct Row {
    pub(crate) index: Option<String>,
    pub(crate) id: String,
    pub(crate) score: f64,
    #[serde(default)]
    pub(crate) explanation: Option<Box<RawValue>>,
    #[serde(default)]
    pub(crate) locations: Option<RowLocations>,
    #[serde(default)]
    pub(crate) fragments: Option<HashMap<String, Vec<String>>>,
    #[serde(default)]
    pub(crate) fields: Option<Box<RawValue>>,
}

impl From<Row> for ResultHit {
    fn from(row: Row) -> ResultHit {
        let locations = if let Some(row_locations) = row.locations {
            let mut locations = HashMap::new();
            for (field_name, field_data) in row_locations {
                let mut terms = HashMap::new();
                for (term_name, term_data) in field_data {
                    let mut term_locations = Vec::with_capacity(term_data.len());
                    for (loc_idx, loc_data) in term_data.into_iter().enumerate() {
                        term_locations.push(HitLocation {
                            position: loc_idx as u32,
                            start: loc_data.start,
                            end: loc_data.end,
                            array_positions: loc_data.array_positions,
                        });
                    }
                    terms.insert(term_name, term_locations);
                }
                locations.insert(field_name, terms);
            }
            Some(locations)
        } else {
            None
        };

        ResultHit {
            index: row.index.unwrap_or_default(),
            id: row.id,
            score: row.score,
            locations,
            fragments: row.fragments,
            fields: row.fields,
            explanation: row.explanation,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponse {
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DocumentAnalysisJson {
    pub status: String,
    pub analyzed: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IndexedDocumentsJson {
    pub count: u64,
}
