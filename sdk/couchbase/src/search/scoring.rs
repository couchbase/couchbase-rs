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

//! Scoring definitions for Full-Text Search results, including score fusion.
//!
//! Score fusion controls how the FTS and vector result sets of a hybrid search are merged
//! into a single ranked list. It is only meaningful for a hybrid request (both an FTS query
//! and a vector search); applied to a single result set, it re-scores the hits but leaves
//! their ordering unchanged.
//!
//! Use the [`Scoring`] enum to pass a scoring mode to
//! [`SearchOptions::scoring`](crate::options::search_options::SearchOptions::scoring).
//!
//! **Volatile: This API is subject to change at any time.**

/// Selects how search results are scored.
///
/// Use with [`SearchOptions::scoring`](crate::options::search_options::SearchOptions::scoring).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scoring {
    /// Disables scoring entirely. Works on any server version.
    None,
    /// Reciprocal Rank Fusion: merges by rank rather than raw score.
    ReciprocalRankFusion(ReciprocalRankScoreFusion),
    /// Relative Score Fusion: merges by normalized score rather than rank.
    RelativeScoreFusion(RelativeScoreScoreFusion),
}

/// Tuning parameters for [`Scoring::ReciprocalRankFusion`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReciprocalRankScoreFusion {
    /// The rank constant used when merging by rank.
    pub rank_constant: Option<u32>,
    /// How many results per list are considered for fusion.
    pub window_size: Option<u32>,
}

impl ReciprocalRankScoreFusion {
    /// Creates a new `ReciprocalRankScoreFusion` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the rank constant.
    pub fn rank_constant(mut self, rank_constant: u32) -> Self {
        self.rank_constant = Some(rank_constant);
        self
    }

    /// Sets how many results per list are considered for fusion.
    pub fn window_size(mut self, window_size: u32) -> Self {
        self.window_size = Some(window_size);
        self
    }
}

/// Tuning parameters for [`Scoring::RelativeScoreFusion`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RelativeScoreScoreFusion {
    /// How many results per list are considered for fusion.
    pub window_size: Option<u32>,
}

impl RelativeScoreScoreFusion {
    /// Creates a new `RelativeScoreScoreFusion` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how many results per list are considered for fusion.
    pub fn window_size(mut self, window_size: u32) -> Self {
        self.window_size = Some(window_size);
        self
    }
}
