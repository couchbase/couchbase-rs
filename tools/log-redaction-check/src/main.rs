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

//! Usage: `cargo run -p log-redaction-check [PATH...]`
//!
//! Checks the SDK sources by default, or the given files and directories. Exits non-zero while any
//! log argument is unannotated.

use log_redaction_check::{check_source, Allowlist};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::{env, fs};

const DEFAULT_PATHS: [&str; 3] = [
    "sdk/couchbase-core/src",
    "sdk/couchbase/src",
    "sdk/couchbase-connstr/src",
];

const ALLOWLIST: &str = include_str!("../allowlist.txt");

fn main() -> ExitCode {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let args: Vec<PathBuf> = env::args().skip(1).map(PathBuf::from).collect();
    let roots = if args.is_empty() {
        DEFAULT_PATHS
            .iter()
            .map(|p| workspace_root.join(p))
            .collect()
    } else {
        args
    };

    let mut files = vec![];
    for root in &roots {
        if !root.exists() {
            eprintln!("{}: not found", root.display());
            return ExitCode::from(2);
        }
        collect_rust_files(root, &mut files);
    }
    files.sort();

    let allowlist = Allowlist::parse(ALLOWLIST);
    let mut count = 0;
    for file in &files {
        let source = match fs::read_to_string(file) {
            Ok(source) => source,
            Err(e) => {
                eprintln!("{}: failed to read: {e}", file.display());
                return ExitCode::from(2);
            }
        };
        let findings = match check_source(&source, &allowlist) {
            Ok(findings) => findings,
            Err(e) => {
                eprintln!("{}: failed to parse: {e}", file.display());
                return ExitCode::from(2);
            }
        };
        let display = file.strip_prefix(&workspace_root).unwrap_or(file);
        for finding in findings {
            println!("{}:{}: {finding}", display.display(), finding.line);
            count += 1;
        }
    }

    if count == 0 {
        println!(
            "{} files checked, every log argument is annotated and every span is under couchbase::tracing",
            files.len()
        );
        return ExitCode::SUCCESS;
    }

    println!(
        "\n{count} finding(s). Wrap each unannotated argument with a helper from \
         couchbase_core::log_redaction (user_data, metadata, system_data, system_data_list), or \
         mark a reviewed decision with not_sensitive or not_redacted. A name that only ever holds \
         SDK-generated values or protocol constants can instead be added to \
         tools/log-redaction-check/allowlist.txt, with a comment saying why. Give every span \
         target \"couchbase::tracing\", so that the filter in the ClusterOptions::log_redaction \
         docs keeps its fields out of log files."
    );
    ExitCode::FAILURE
}

fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_file() {
        if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path.to_path_buf());
        }
        return;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        collect_rust_files(&entry.path(), files);
    }
}
