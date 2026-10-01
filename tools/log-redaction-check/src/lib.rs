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

//! Checks that every value passed to a `tracing` macro in the SDK is annotated for log redaction.
//!
//! An argument passes when it is one of:
//!
//! * a literal, or a constant (a `SCREAMING_CASE` name, or a path such as `RetryReason::NotReady`);
//! * a call to one of the wrappers in `couchbase_core::log_redaction`, including the exclusion
//!   markers `not_sensitive` and `not_redacted`, which record a reviewed decision at the call site;
//! * a value whose name is on the allowlist in `allowlist.txt`: names that hold only values the
//!   SDK generated or protocol constants, wherever they appear.
//!
//! Anything else is reported, so that every new log argument gets a decision. This is an allowlist
//! rather than a list of sensitive-looking names, because a sensitive value with an unremarkable
//! name, such as an error logged as `{e}`, is exactly what a list of sensitive names misses.
//!
//! The check reads names, not types. It cannot see what a value composed before the log statement
//! contains, so build the message inside the log statement rather than with `format!` ahead of it.
//!
//! Two kinds of `tracing` output are telemetry rather than log lines, and are left untagged on
//! purpose, because tracing and metrics backends need the real values:
//!
//! * Span fields. Every span the SDK creates, with a span macro or `#[instrument]`, must set
//!   `target: "couchbase::tracing"`, so that one filter keeps all of them out of a log file. A span
//!   without that target is reported.
//! * Metric events, sent with `event!` to the `couchbase::metrics` target. Their arguments are not
//!   checked.

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeSet, HashSet};
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Lit, Macro};

/// The `tracing` macros that format their arguments into a log line.
const LOG_MACROS: [&str; 6] = ["trace", "debug", "info", "warn", "error", "event"];

/// The `tracing` macros that create a span.
const SPAN_MACROS: [&str; 6] = [
    "span",
    "trace_span",
    "debug_span",
    "info_span",
    "warn_span",
    "error_span",
];

/// The target every SDK span must use, so that one filter covers all of them.
const SPAN_TARGET: &str = "couchbase::tracing";

/// The target of metric events, which are telemetry rather than log lines.
const METRICS_TARGET: &str = "couchbase::metrics";

/// The wrappers from `couchbase_core::log_redaction`. An argument built from one is annotated.
const WRAPPERS: [&str; 6] = [
    "user_data",
    "metadata",
    "system_data",
    "system_data_list",
    "not_sensitive",
    "not_redacted",
];

/// Macros that expand to a constant, so an argument built from one needs no annotation.
const CONSTANT_MACROS: [&str; 4] = ["env", "concat", "stringify", "file"];

/// A log argument that is not annotated for redaction, or a span outside the SDK's span target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub line: usize,
    pub kind: FindingKind,
    pub macro_name: String,
    pub argument: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindingKind {
    /// A log argument with no redaction annotation.
    Unannotated,
    /// A span that does not set the SDK's span target.
    SpanTarget,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            FindingKind::Unannotated => write!(
                f,
                "{}! argument `{}` is not annotated for log redaction",
                self.macro_name, self.argument
            ),
            FindingKind::SpanTarget => write!(
                f,
                "{} span does not set target \"{SPAN_TARGET}\"",
                self.macro_name
            ),
        }
    }
}

/// The names of values that are safe to log untagged wherever they appear.
pub struct Allowlist {
    names: HashSet<String>,
}

impl Allowlist {
    /// Parses an allowlist: one name per line, with everything after `#` a comment.
    pub fn parse(contents: &str) -> Self {
        let names = contents
            .lines()
            .map(|line| line.split('#').next().unwrap_or_default().trim())
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect();
        Self { names }
    }

    fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }
}

/// Checks one source file, returning every unannotated log argument in it.
pub fn check_source(source: &str, allowlist: &Allowlist) -> syn::Result<Vec<Finding>> {
    let file = syn::parse_file(source)?;
    let mut checker = Checker {
        allowlist,
        findings: vec![],
    };
    checker.visit_file(&file);
    Ok(checker.findings)
}

struct Checker<'a> {
    allowlist: &'a Allowlist,
    findings: Vec<Finding>,
}

impl<'ast> Visit<'ast> for Checker<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !is_test_only(&item.attrs) {
            self.check_instrument(&item.attrs);
            visit::visit_item_fn(self, item);
        }
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !is_test_only(&item.attrs) {
            self.check_instrument(&item.attrs);
            visit::visit_impl_item_fn(self, item);
        }
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        if !is_test_only(&item.attrs) {
            self.check_instrument(&item.attrs);
            visit::visit_trait_item_fn(self, item);
        }
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        if !is_test_only(&item.attrs) {
            visit::visit_item_impl(self, item);
        }
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        let Some(name) = mac.path.segments.last().map(|s| s.ident.to_string()) else {
            return;
        };
        let line = mac.path.segments[0].ident.span().start().line;
        self.check_macro(&name, line, mac.tokens.clone());
    }
}

impl Checker<'_> {
    fn check_macro(&mut self, name: &str, line: usize, tokens: TokenStream) {
        if LOG_MACROS.contains(&name) {
            self.check_log_call(name, line, tokens.clone());
        } else if SPAN_MACROS.contains(&name)
            && directive_target(&tokens).as_deref() != Some(SPAN_TARGET)
        {
            self.report(
                FindingKind::SpanTarget,
                &format!("{name}!"),
                line,
                String::new(),
            );
        }
        // syn leaves the body of every macro as raw tokens, so log calls inside one, whether
        // tokio::select! or the arguments of a log or span macro, are found by scanning for
        // `name!(...)`.
        self.scan_for_nested_macros(tokens);
    }

    fn scan_for_nested_macros(&mut self, tokens: TokenStream) {
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        for (idx, tree) in trees.iter().enumerate() {
            match tree {
                TokenTree::Punct(p) if p.as_char() == '!' && p.spacing() == Spacing::Alone => {
                    if let (Some(TokenTree::Ident(name)), Some(TokenTree::Group(group))) =
                        (idx.checked_sub(1).map(|i| &trees[i]), trees.get(idx + 1))
                    {
                        self.check_macro(
                            &name.to_string(),
                            name.span().start().line,
                            group.stream(),
                        );
                    }
                }
                TokenTree::Group(group) => {
                    // A macro's own argument group is handled when its `!` is reached.
                    let is_macro_args = idx > 0
                        && matches!(&trees[idx - 1], TokenTree::Punct(p) if p.as_char() == '!');
                    if !is_macro_args {
                        self.scan_for_nested_macros(group.stream());
                    }
                }
                _ => {}
            }
        }
    }

    // `#[instrument]` creates a span, so it has to set the SDK's span target like a span macro.
    fn check_instrument(&mut self, attrs: &[Attribute]) {
        for attr in attrs {
            let is_instrument = attr
                .path()
                .segments
                .last()
                .is_some_and(|s| s.ident == "instrument");
            if !is_instrument {
                continue;
            }
            let has_target = match &attr.meta {
                syn::Meta::List(list) => split_arguments(list.tokens.clone())
                    .iter()
                    .any(|item| item.to_string() == format!("target = \"{SPAN_TARGET}\"")),
                _ => false,
            };
            if !has_target {
                let line = attr.pound_token.span.start().line;
                self.report(
                    FindingKind::SpanTarget,
                    "#[instrument]",
                    line,
                    String::new(),
                );
            }
        }
    }

    fn report(&mut self, kind: FindingKind, macro_name: &str, line: usize, argument: String) {
        self.findings.push(Finding {
            line,
            kind,
            macro_name: macro_name.to_string(),
            argument,
        });
    }

    fn check_log_call(&mut self, macro_name: &str, line: usize, tokens: TokenStream) {
        if directive_target(&tokens).as_deref() == Some(METRICS_TARGET) {
            return;
        }

        let mut seen_format_string = false;
        let mut named_args = HashSet::new();
        let mut captures = BTreeSet::new();

        for item in split_arguments(tokens) {
            if is_directive(&item) {
                continue;
            }

            if !seen_format_string {
                if let Some(format) = as_string_literal(&item) {
                    seen_format_string = true;
                    captures = inline_captures(&format);
                    continue;
                }
                // Before the format string come fields: `name = value`, `%value`, `?value`, or a
                // bare `value`, where the value may itself carry a `%` or `?`.
                let value = strip_field_sigil(field_value(item));
                match syn::parse2::<Expr>(value.clone()) {
                    Ok(expr) => self.check_argument(macro_name, line, &expr),
                    Err(_) => self.report_unparsed(macro_name, line, &value),
                }
                continue;
            }

            match syn::parse2::<Expr>(item.clone()) {
                Err(_) => self.report_unparsed(macro_name, line, &item),
                Ok(expr) => match expr {
                    Expr::Assign(assign) => {
                        if let Expr::Path(path) = assign.left.as_ref() {
                            if let Some(ident) = path.path.get_ident() {
                                named_args.insert(ident.to_string());
                            }
                        }
                        self.check_argument(macro_name, line, &assign.right);
                    }
                    expr => self.check_argument(macro_name, line, &expr),
                },
            }
        }

        // A name captured inline, as in "{e}", is an argument the format string reads directly,
        // unless the call also passes it as a named argument, which was checked above.
        for name in captures.difference(&named_args.into_iter().collect()) {
            if !is_constant_name(name) && !self.allowlist.contains(name) {
                self.report(FindingKind::Unannotated, macro_name, line, name.clone());
            }
        }
    }

    // An argument the checker cannot read is reported rather than passed, so that the check fails
    // closed.
    fn report_unparsed(&mut self, macro_name: &str, line: usize, tokens: &TokenStream) {
        self.report(
            FindingKind::Unannotated,
            macro_name,
            line,
            tokens.to_string(),
        );
    }

    fn check_argument(&mut self, macro_name: &str, line: usize, expr: &Expr) {
        if !self.is_annotated(expr) {
            let argument = expr.to_token_stream().to_string();
            self.report(FindingKind::Unannotated, macro_name, line, argument);
        }
    }

    fn is_annotated(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Reference(e) => self.is_annotated(&e.expr),
            Expr::Paren(e) => self.is_annotated(&e.expr),
            Expr::Group(e) => self.is_annotated(&e.expr),
            Expr::Cast(e) => self.is_annotated(&e.expr),
            Expr::Unary(e) if matches!(e.op, syn::UnOp::Deref(_)) => self.is_annotated(&e.expr),
            Expr::Lit(_) => true,
            Expr::Path(e) => match e.path.segments.last() {
                Some(last) => {
                    let name = last.ident.to_string();
                    let is_variant = e.path.segments.len() > 1
                        && name.starts_with(|c: char| c.is_ascii_uppercase());
                    is_constant_name(&name) || is_variant || self.allowlist.contains(&name)
                }
                None => false,
            },
            Expr::Field(e) => match &e.member {
                syn::Member::Named(ident) => self.allowlist.contains(&ident.to_string()),
                syn::Member::Unnamed(_) => false,
            },
            Expr::Call(e) => match call_name(&e.func) {
                Some(name) => WRAPPERS.contains(&name.as_str()) || self.allowlist.contains(&name),
                None => false,
            },
            Expr::MethodCall(e) => {
                is_wrapped_receiver(&e.receiver) || self.allowlist.contains(&e.method.to_string())
            }
            Expr::Macro(e) => e
                .mac
                .path
                .get_ident()
                .is_some_and(|ident| CONSTANT_MACROS.contains(&ident.to_string().as_str())),
            _ => false,
        }
    }
}

/// Whether a method chain starts from a wrapper, as in `system_data_list(keys).quoted()`.
fn is_wrapped_receiver(expr: &Expr) -> bool {
    match expr {
        Expr::MethodCall(e) => is_wrapped_receiver(&e.receiver),
        Expr::Call(e) => call_name(&e.func).is_some_and(|name| WRAPPERS.contains(&name.as_str())),
        _ => false,
    }
}

fn call_name(func: &Expr) -> Option<String> {
    match func {
        Expr::Path(path) => path.path.segments.last().map(|s| s.ident.to_string()),
        _ => None,
    }
}

/// Whether an item only exists in tests: it is a `#[test]` (or `#[tokio::test]`), or a `#[cfg]`
/// requires `test`. Anything the predicate cannot prove to be test-only is checked, so that a
/// `#[cfg(not(test))]` or a feature name containing "test" is never skipped.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        let path = attr.path();
        if path.segments.last().is_some_and(|s| s.ident == "test") {
            return true;
        }
        path.is_ident("cfg")
            && attr
                .parse_args::<syn::Meta>()
                .is_ok_and(|meta| cfg_requires_test(&meta))
    })
}

fn cfg_requires_test(meta: &syn::Meta) -> bool {
    match meta {
        syn::Meta::Path(path) => path.is_ident("test"),
        // all(...) holds only if every predicate does, so it requires test if any of them does.
        // any(...) and not(...) do not.
        syn::Meta::List(list) if list.path.is_ident("all") => list
            .parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            )
            .is_ok_and(|predicates| predicates.iter().any(cfg_requires_test)),
        _ => false,
    }
}

/// The value of a `target: "..."` directive at the top level of a macro's arguments.
fn directive_target(tokens: &TokenStream) -> Option<String> {
    split_arguments(tokens.clone())
        .into_iter()
        .find_map(|item| {
            if !is_directive(&item) {
                return None;
            }
            let trees: Vec<TokenTree> = item.into_iter().collect();
            match trees.as_slice() {
                [TokenTree::Ident(ident), TokenTree::Punct(_), rest @ ..] if ident == "target" => {
                    as_string_literal(&rest.iter().cloned().collect())
                }
                _ => None,
            }
        })
}

fn is_constant_name(name: &str) -> bool {
    name.chars().any(|c| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Splits a macro's tokens at top-level commas. Commas inside a group belong to that group.
fn split_arguments(tokens: TokenStream) -> Vec<TokenStream> {
    let mut items = vec![];
    let mut current = TokenStream::new();
    for tree in tokens {
        match &tree {
            TokenTree::Punct(p) if p.as_char() == ',' => {
                items.push(std::mem::take(&mut current));
            }
            _ => current.extend([tree]),
        }
    }
    if !current.is_empty() {
        items.push(current);
    }
    items
}

/// Whether an item is a `target:`, `parent:` or `name:` directive rather than a value.
fn is_directive(item: &TokenStream) -> bool {
    let trees: Vec<TokenTree> = item.clone().into_iter().take(2).collect();
    matches!(
        trees.as_slice(),
        [TokenTree::Ident(ident), TokenTree::Punct(p)]
            if p.as_char() == ':' && p.spacing() == Spacing::Alone
                && ["target", "parent", "name"].contains(&ident.to_string().as_str())
    )
}

fn as_string_literal(item: &TokenStream) -> Option<String> {
    let trees: Vec<TokenTree> = item.clone().into_iter().collect();
    match trees.as_slice() {
        [TokenTree::Literal(literal)] => match Lit::new(literal.clone()) {
            Lit::Str(s) => Some(s.value()),
            _ => None,
        },
        // A literal produced by a macro, such as a format string from concat!, arrives wrapped in
        // an invisible group.
        [TokenTree::Group(group)] if group.delimiter() == Delimiter::None => {
            as_string_literal(&group.stream())
        }
        _ => None,
    }
}

/// The value of a field: what follows a top-level `=` in `name = value`, or the whole item when
/// there is no `=`.
fn field_value(item: TokenStream) -> TokenStream {
    let trees: Vec<TokenTree> = item.clone().into_iter().collect();
    let assign = trees.iter().enumerate().position(|(idx, tree)| {
        let is_eq = matches!(tree, TokenTree::Punct(p) if p.as_char() == '=' && p.spacing() == Spacing::Alone);
        // `==`, `<=`, `>=` and `!=` are comparisons, not field assignments.
        let after_punct = idx > 0
            && matches!(&trees[idx - 1], TokenTree::Punct(p) if p.spacing() == Spacing::Joint);
        is_eq && !after_punct
    });
    match assign {
        Some(idx) => trees[idx + 1..].iter().cloned().collect(),
        None => item,
    }
}

fn strip_field_sigil(item: TokenStream) -> TokenStream {
    let mut trees = item.into_iter().peekable();
    if let Some(TokenTree::Punct(p)) = trees.peek() {
        if p.as_char() == '%' || p.as_char() == '?' {
            trees.next();
        }
    }
    trees.collect()
}

/// The names a format string captures inline, such as `e` in `"failed: {e}"` or `"{e:?}"`.
fn inline_captures(format: &str) -> BTreeSet<String> {
    let format = format.replace("{{", "").replace("}}", "");
    let mut captures = BTreeSet::new();
    let mut rest = format.as_str();
    while let Some(start) = rest.find('{') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('}') else {
            break;
        };
        let spec = &rest[..end];
        let name = spec.split(':').next().unwrap_or_default().trim();
        if name.starts_with(|c: char| c.is_alphabetic() || c == '_') {
            captures.insert(name.to_string());
        }
        rest = &rest[end + 1..];
    }
    captures
}

#[cfg(test)]
mod tests {
    use super::*;

    fn findings(body: &str) -> Vec<String> {
        let allowlist = Allowlist::parse("id # generated\nopaque\nlen\n");
        let source = format!("fn f() {{ {body} }}");
        check_source(&source, &allowlist)
            .unwrap()
            .into_iter()
            .map(|f| f.argument)
            .collect()
    }

    #[test]
    fn a_wrapped_argument_passes() {
        assert!(findings(r#"warn!("failed {}", user_data(&e));"#).is_empty());
        assert!(findings(r#"debug!("bucket {}", metadata(&bucket));"#).is_empty());
        assert!(findings(r#"debug!("host {}", system_data(host));"#).is_empty());
        assert!(findings(r#"info!("features {:?}", not_sensitive(&features));"#).is_empty());
        assert!(findings(r#"trace!("config {:?}", not_redacted(&config));"#).is_empty());
    }

    #[test]
    fn a_qualified_wrapper_passes() {
        assert!(findings(r#"warn!("failed {}", log_redaction::user_data(&e));"#).is_empty());
    }

    #[test]
    fn a_wrapped_list_with_builder_calls_passes() {
        assert!(
            findings(r#"debug!("endpoints [{}]", system_data_list(map.keys()).quoted());"#)
                .is_empty()
        );
    }

    #[test]
    fn an_unwrapped_argument_is_reported() {
        assert_eq!(findings(r#"warn!("failed {}", e);"#), vec!["e"]);
        assert_eq!(
            findings(r#"debug!("bucket {}", &bucket_name);"#),
            vec!["& bucket_name"]
        );
    }

    #[test]
    fn an_inline_capture_is_reported() {
        assert_eq!(findings(r#"warn!("failed {e}");"#), vec!["e"]);
        assert_eq!(findings(r#"warn!("failed {e:?}");"#), vec!["e"]);
    }

    #[test]
    fn escaped_braces_are_not_captures() {
        assert!(findings(r#"warn!("{{ literal }}");"#).is_empty());
    }

    #[test]
    fn an_allowlisted_name_passes() {
        assert!(findings(r#"debug!("client {} opaque {opaque}", self.id);"#).is_empty());
        assert!(findings(r#"debug!("extras {}", extras.len());"#).is_empty());
    }

    #[test]
    fn literals_and_constants_pass() {
        assert!(findings(r#"debug!("{} {}", 1, "x");"#).is_empty());
        assert!(findings(r#"warn!("{DEBUG_CONFIG_ENV_VAR} is set");"#).is_empty());
        assert!(findings(r#"debug!("{}", RetryReason::NotReady);"#).is_empty());
        assert!(findings(r#"info!("version {}", env!("CARGO_PKG_VERSION"));"#).is_empty());
    }

    #[test]
    fn a_named_argument_is_checked_by_its_value() {
        assert!(findings(r#"info!("rev {rev}", rev = config.opaque);"#).is_empty());
        assert_eq!(
            findings(r#"info!("rev {rev}", rev = config.bucket);"#),
            vec!["config . bucket"]
        );
    }

    #[test]
    fn a_message_composed_ahead_of_the_log_statement_is_reported() {
        assert_eq!(findings(r#"warn!("{msg}");"#), vec!["msg"]);
        assert_eq!(
            findings(r#"warn!("{}", format!("x {}", e));"#),
            vec![r#"format ! ("x {}" , e)"#]
        );
    }

    #[test]
    fn fields_and_directives_are_handled() {
        assert!(findings(r#"debug!(target: "couchbase", "message");"#).is_empty());
        assert_eq!(
            findings(r#"debug!(target: "couchbase", bucket = %name, "message");"#),
            vec!["name"]
        );
        assert!(findings(r#"debug!(id = %self.id, "message");"#).is_empty());
        assert!(findings(r#"debug!(opaque = ?packet.opaque, "message");"#).is_empty());
        assert_eq!(findings(r#"debug!(?config, "message");"#), vec!["config"]);
    }

    #[test]
    fn a_lowercase_qualified_path_is_reported() {
        assert_eq!(
            findings(r#"debug!("{}", config::endpoint);"#),
            vec!["config :: endpoint"]
        );
        assert!(findings(r#"debug!("{}", Self::TIMEOUT);"#).is_empty());
        assert!(findings(r#"debug!("{}", crate::retry::RetryReason::NotReady);"#).is_empty());
        assert!(findings(r#"debug!("{}", config::opaque);"#).is_empty());
    }

    #[test]
    fn a_log_call_inside_a_log_or_span_argument_is_found() {
        assert_eq!(
            findings(
                r#"let s = tracing::trace_span!(target: "couchbase::tracing", "s", field = { warn!("{secret}"); 42 });"#
            ),
            vec!["secret"]
        );
        assert_eq!(
            findings(r#"debug!("{}", { warn!("{secret}"); 1 });"#),
            vec!["{ warn ! (\"{secret}\") ; 1 }", "secret"]
        );
    }

    #[test]
    fn an_argument_that_cannot_be_parsed_is_reported() {
        assert_eq!(findings(r#"warn!("failed {}", @e);"#), vec!["@ e"]);
    }

    #[test]
    fn a_log_call_inside_another_macro_is_found() {
        assert_eq!(
            findings(
                r#"tokio::select! { _ = tick.tick() => { warn!("failed {}", e); } x = rx => {} }"#
            ),
            vec!["e"]
        );
    }

    #[test]
    fn a_qualified_log_macro_is_found() {
        assert_eq!(findings(r#"tracing::error!("failed {e}");"#), vec!["e"]);
    }

    #[test]
    fn other_macros_are_ignored() {
        assert!(findings(r#"let s = format!("{}", e); assert!(x != y);"#).is_empty());
    }

    fn kinds(source: &str) -> Vec<(FindingKind, String)> {
        let allowlist = Allowlist::parse("");
        check_source(source, &allowlist)
            .unwrap()
            .into_iter()
            .map(|f| (f.kind, f.macro_name))
            .collect()
    }

    #[test]
    fn an_event_is_checked_like_a_log_macro() {
        assert_eq!(
            findings(r#"tracing::event!(Level::WARN, "failed {}", doc_key);"#),
            vec!["doc_key"]
        );
        assert!(findings(r#"tracing::event!(tracing::Level::WARN, "failed");"#).is_empty());
    }

    #[test]
    fn a_metric_event_is_not_checked() {
        assert!(findings(
            r#"tracing::event!(target: "couchbase::metrics", Level::TRACE, db.namespace = bucket);"#
        )
        .is_empty());
    }

    #[test]
    fn a_metric_event_inside_a_macro_definition_is_not_checked() {
        let source = r#"
            macro_rules! record {
                ($value:expr) => {
                    tracing::event!(target: "couchbase::metrics", Level::TRACE, x = $value)
                };
            }
        "#;
        assert!(kinds(source).is_empty());
    }

    #[test]
    fn a_span_without_the_sdk_target_is_reported() {
        assert_eq!(
            kinds(r#"fn f() { let s = tracing::trace_span!("get", db.namespace = b); }"#),
            vec![(FindingKind::SpanTarget, "trace_span!".to_string())]
        );
        assert_eq!(
            kinds(r#"fn f() { let s = trace_span!(target: "couchbase_core", "get"); }"#),
            vec![(FindingKind::SpanTarget, "trace_span!".to_string())]
        );
    }

    #[test]
    fn a_span_with_the_sdk_target_passes() {
        assert!(kinds(
            r#"fn f() { let s = tracing::trace_span!(target: "couchbase::tracing", "get", x = y); }"#
        )
        .is_empty());
    }

    #[test]
    fn instrument_without_the_sdk_target_is_reported() {
        let source = r#"
            impl Q {
                #[instrument(skip_all, level = Level::TRACE, name = "query")]
                async fn query(&self) {}
            }
        "#;
        assert_eq!(
            kinds(source),
            vec![(FindingKind::SpanTarget, "#[instrument]".to_string())]
        );
    }

    #[test]
    fn instrument_on_a_default_trait_method_without_the_sdk_target_is_reported() {
        let source = r#"
            trait Q {
                #[instrument(skip_all, name = "query")]
                async fn query(&self) {}
            }
        "#;
        assert_eq!(
            kinds(source),
            vec![(FindingKind::SpanTarget, "#[instrument]".to_string())]
        );
    }

    #[test]
    fn instrument_with_the_sdk_target_passes() {
        let source = r#"
            #[tracing::instrument(target = "couchbase::tracing", skip_all, name = "query")]
            async fn query() {}
        "#;
        assert!(kinds(source).is_empty());
    }

    #[test]
    fn production_code_under_a_cfg_mentioning_test_is_checked() {
        let allowlist = Allowlist::parse("");
        for cfg in [
            "#[cfg(not(test))]",
            r#"#[cfg(feature = "unstable-test-hooks")]"#,
            "#[cfg(any(test, feature = \"x\"))]",
        ] {
            let source = format!("{cfg} fn f() {{ warn!(\"{{}}\", doc_key); }}");
            assert_eq!(
                check_source(&source, &allowlist).unwrap().len(),
                1,
                "{cfg} is not test-only"
            );
        }
    }

    #[test]
    fn code_that_requires_test_is_skipped() {
        let allowlist = Allowlist::parse("");
        for attr in [
            "#[cfg(test)]",
            "#[cfg(all(test, feature = \"x\"))]",
            "#[test]",
            "#[tokio::test]",
        ] {
            let source = format!("{attr} fn f() {{ warn!(\"{{}}\", doc_key); }}");
            assert!(
                check_source(&source, &allowlist).unwrap().is_empty(),
                "{attr} is test-only"
            );
        }
    }

    #[test]
    fn test_code_is_skipped() {
        let allowlist = Allowlist::parse("");
        let source = r#"
            #[cfg(test)]
            mod tests { fn f() { warn!("{}", e); } }
            #[test]
            fn t() { warn!("{}", e); }
        "#;
        assert!(check_source(source, &allowlist).unwrap().is_empty());
    }

    #[test]
    fn findings_carry_the_line() {
        let allowlist = Allowlist::parse("");
        let source = "fn f() {\n\n    warn!(\"failed {e}\");\n}\n";
        let findings = check_source(source, &allowlist).unwrap();
        assert_eq!(findings[0].line, 3);
        assert_eq!(findings[0].macro_name, "warn");
    }

    #[test]
    fn the_allowlist_ignores_comments_and_blank_lines() {
        let allowlist = Allowlist::parse("# header\n\nid  # generated\n");
        assert!(allowlist.contains("id"));
        assert!(!allowlist.contains("header"));
    }
}
