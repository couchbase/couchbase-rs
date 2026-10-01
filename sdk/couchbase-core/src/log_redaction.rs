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

//! Log redaction annotations.
//!
//! The SDK never removes or obscures anything itself. It wraps sensitive values in fixed tags so
//! that an external tool can strip or hash them after the fact. Three categories exist:
//!
//! | Tag | Category | Examples |
//! |-----|----------|----------|
//! | `<ud>...</ud>` | user data | document keys and values, application usernames, query statements, document xattrs |
//! | `<md>...</md>` | metadata | cluster names, bucket/scope/collection names, design document, view and index names |
//! | `<sd>...</sd>` | system data | IP addresses, hostnames, ports, DNS topology |
//!
//! Redaction is off by default and is process-wide. It is enabled with [`set_log_redaction`], or
//! by opening an agent whose options ask for it. Opening an agent can only turn redaction on,
//! never off, so that one agent cannot silently disable it for another.
//!
//! # Usage at a log site
//!
//! ```rust
//! use couchbase_core::log_redaction::{system_data, user_data};
//! # let host = "10.0.0.1";
//! # let key = "airline_10";
//! tracing::info!("connecting to {}", system_data(&host));
//! tracing::debug!("fetching {}", user_data(&key));
//! ```
//!
//! # Choosing a category
//!
//! In order:
//!
//! * A literal from this source takes no tag: an opcode name, a status, an error code, a
//!   duration, a retry count, an id the SDK generated. It reads the same for every deployment and
//!   identifies nobody.
//! * A secret takes no tag either, because it must not be logged at all: a password, a bearer
//!   token, a TLS private key, a SASL payload. A tag would be no protection here, since redaction
//!   is off by default and a credential that reaches a log file has already left the process.
//!   There is deliberately no wrapper for this case.
//! * Otherwise the origin of the value decides. Something the application supplied is user data,
//!   a Couchbase resource name is metadata, a host or the network path to it is system data. A
//!   value that came from outside the SDK and fits none of the three is user data, because the
//!   fallback has to be the strictest category rather than no tag at all.
//!
//! A value that mixes categories takes one tag, around the whole value, at the strictest category
//! it contains. An error message can quote a query statement or carry a request URL, so an error
//! is tagged whole as user data. Reaching inside a value to tag only the part of it that is
//! sensitive is where this feature gets implemented wrong: the boundaries move with the format,
//! and a miss is silent.
//!
//! A serialized document goes the same way: annotate the whole argument, or none of it, but never
//! a value inside it. A tag written into a JSON string value is read back as part of that value by
//! anything that parses the line.
//!
//! A list of like-for-like values gets one tag per entry rather than one around the whole list, so
//! that an entry still matches the same value logged on its own elsewhere. See
//! [`system_data_list`].
//!
//! # Where a tag goes
//!
//! A tag belongs to a log line and nowhere else, so it goes on in the log statement rather than in
//! the code that produced the value. Nothing headed for the wire, into a config, or back to the
//! caller may carry one, which means no getter and no `Display` implementation of an SDK type
//! returns a tagged value: those same implementations are reached from places a tag would corrupt,
//! such as an application formatting an error.
//!
//! # What a tagged value may not contain
//!
//! Two things must never appear inside a tagged value, and the wrappers remove both while
//! redaction is enabled, so that no call site has to remember.
//!
//! * A newline. The tool that consumes these tags is line-oriented, so a value that spans lines
//!   leaves its opening and closing tags on different lines, and the value is then passed through
//!   in clear text. Newlines and carriage returns become `\n` and `\r`.
//! * A tag sequence. A value containing `</ud>` closes its own span early, and the rest of it is
//!   then outside any tag. Only the `<` of such a sequence is escaped, as `\u003c`, and only when
//!   it really begins one, so ordinary markup in a logged body stays readable.
//!
//! # Exclusions
//!
//! Two wrappers record a reviewed decision not to tag a value. Neither changes what is printed:
//!
//! * [`not_sensitive`]: the name suggests a category the value does not belong to.
//! * [`not_redacted`]: sensitive, and deliberately logged untagged anyway. Say why at the call
//!   site. The configuration dump behind `RSCBC_DEBUG_CONFIG` is one: it exists to show exactly
//!   what crossed the wire, and it is an opt-in debugging aid rather than something a deployment
//!   leaves on.
//!
//! # Checking
//!
//! CI runs `cargo run -p log-redaction-check`, which reports every argument to a `tracing` macro
//! that is not wrapped with a helper from this module or named on the checker's allowlist of
//! SDK-generated and protocol values (`tools/log-redaction-check/allowlist.txt`). It reads names,
//! not types, so it cannot see inside a message composed with `format!` ahead of the log
//! statement: build the message in the log statement itself.
//!
//! # Formatting
//!
//! When redaction is disabled the wrappers format exactly as the underlying value, so annotating a
//! log statement never changes its output for users who have not opted in. When it is enabled,
//! width, alignment, precision and the alternate flag apply to the value alone, so `{:>8}` pads
//! inside the tags. The fill character, sign and zero-padding flags are not carried inside a tag.

use std::fmt::{self, Debug, Display, Formatter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

static LOG_REDACTION_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable log redaction.
///
/// Redaction state is process-wide. When enabled, values wrapped by the helpers in this module
/// are surrounded by `<ud>`/`<md>`/`<sd>` tags so that an external tool can strip or hash them;
/// the SDK never removes or obscures anything itself.
///
/// Enable redaction before connecting, so that every line logged while connecting is tagged.
///
/// Enabling emits a one-time message recording that the log carries redaction tags and still
/// needs processing.
pub fn set_log_redaction(enable: bool) {
    let was_enabled = LOG_REDACTION_ENABLED.swap(enable, Ordering::Relaxed);
    if enable && !was_enabled {
        info!(
            "Log redaction enabled: sensitive values are wrapped in redaction tags. This log still \
             contains identifying information until those tags are processed. Once redacted, \
             diagnosis and support of issues may be challenging or not possible"
        );
    }
}

/// Returns whether log redaction is currently enabled.
pub fn is_log_redaction_enabled() -> bool {
    LOG_REDACTION_ENABLED.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Category {
    User,
    Meta,
    System,
}

impl Category {
    fn tag(self) -> &'static str {
        match self {
            Category::User => "ud",
            Category::Meta => "md",
            Category::System => "sd",
        }
    }
}

/// A value annotated for log redaction. Created by [`user_data`], [`metadata`] and
/// [`system_data`].
///
/// Formats with both `{}` and `{:?}`, as the wrapped value does.
#[derive(Clone, Copy)]
pub struct Redacted<'a, T: ?Sized> {
    value: &'a T,
    category: Category,
}

/// A value deliberately logged without a redaction tag. Created by [`not_sensitive`] and
/// [`not_redacted`]. Formats exactly as the wrapped value does, whether or not redaction is
/// enabled.
#[derive(Clone, Copy)]
pub struct Unredacted<'a, T: ?Sized> {
    value: &'a T,
}

/// Tag a value as user data (`<ud>`): document keys and values, application usernames, query
/// statements, document xattrs.
pub fn user_data<T: ?Sized>(value: &T) -> Redacted<'_, T> {
    Redacted {
        value,
        category: Category::User,
    }
}

/// Tag a value as metadata (`<md>`): cluster, bucket, scope and collection names, design document,
/// view and index names, and other Couchbase resource names.
pub fn metadata<T: ?Sized>(value: &T) -> Redacted<'_, T> {
    Redacted {
        value,
        category: Category::Meta,
    }
}

/// Tag a value as system data (`<sd>`): IP addresses, hostnames, ports and DNS topology.
pub fn system_data<T: ?Sized>(value: &T) -> Redacted<'_, T> {
    Redacted {
        value,
        category: Category::System,
    }
}

/// Mark a value that looks sensitive but is not, because its name suggests a category the value
/// does not belong to. Formats exactly as the value would unwrapped.
pub fn not_sensitive<T: ?Sized>(value: &T) -> Unredacted<'_, T> {
    Unredacted { value }
}

/// Mark a sensitive value that is deliberately logged without a tag. Formats exactly as the value
/// would unwrapped. Unlike [`not_sensitive`] this asserts nothing about the content, so say at the
/// call site why the value carries no tag.
pub fn not_redacted<T: ?Sized>(value: &T) -> Unredacted<'_, T> {
    Unredacted { value }
}

/// The entries of a container, each tagged on its own and joined. Created by
/// [`system_data_list`].
///
/// One tag around the joined list would hash the whole list as a single token, matching nothing
/// else in the log, not even the same value logged beside it in a span of its own.
#[derive(Clone)]
pub struct RedactedList<I> {
    values: I,
    category: Category,
    quoted: bool,
    separator: &'static str,
}

impl<I> RedactedList<I> {
    /// Render each entry inside double quotes. The tag sits inside the quotes, never around them:
    /// a span that swallows the punctuation hashes to something matching no other line.
    pub fn quoted(mut self) -> Self {
        self.quoted = true;
        self
    }

    /// Join the entries with `separator` rather than the default `", "`.
    pub fn separator(mut self, separator: &'static str) -> Self {
        self.separator = separator;
        self
    }
}

/// Tag each entry of a container as system data (`<sd>`) and join them.
pub fn system_data_list<I: IntoIterator + Clone>(values: I) -> RedactedList<I> {
    RedactedList {
        values,
        category: Category::System,
        quoted: false,
        separator: ", ",
    }
}

impl<I> Display for RedactedList<I>
where
    I: IntoIterator + Clone,
    I::Item: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let quote = if self.quoted { "\"" } else { "" };
        for (idx, value) in self.values.clone().into_iter().enumerate() {
            if idx > 0 {
                f.write_str(self.separator)?;
            }
            let entry = Redacted {
                value: &value,
                category: self.category,
            };
            write!(f, "{quote}{entry}{quote}")?;
        }
        Ok(())
    }
}

/// The tag sequences themselves, which must not survive inside a tagged value: a value carrying
/// one would close its own span early, and whatever followed would sit outside any tag.
const TAG_SEQUENCES: [&str; 6] = ["<ud>", "</ud>", "<md>", "</md>", "<sd>", "</sd>"];

/// Writes `value` with everything that would break the span around it replaced.
///
/// A newline splits it: the consumer of these tags is line-oriented. A tag sequence inside the
/// value closes the span early. Only the `<` of such a sequence is escaped, which is enough to
/// break it; escaping it to `\<` would not be, because `\</ud>` still contains `</ud>`.
fn write_escaped(f: &mut Formatter<'_>, value: &str) -> fmt::Result {
    let mut copied = 0;
    for (idx, c) in value.char_indices() {
        let replacement = match c {
            '\n' => "\\n",
            '\r' => "\\r",
            '<' if TAG_SEQUENCES.iter().any(|s| value[idx..].starts_with(s)) => "\\u003c",
            _ => continue,
        };
        f.write_str(&value[copied..idx])?;
        f.write_str(replacement)?;
        copied = idx + c.len_utf8();
    }
    f.write_str(&value[copied..])
}

/// Renders a value into a string, carrying over the width, alignment, precision and alternate
/// flag of `f`. The tagged branch has to render the value before it can escape it, and a
/// `Formatter` cannot be built by hand, so the specification is rebuilt here instead.
macro_rules! render_with_spec {
    ($f:expr, $value:expr, $tr:literal) => {{
        let f: &Formatter<'_> = $f;
        let value = $value;
        let width = f.width().unwrap_or(0);
        let mut out = String::new();
        // Each arm spells out its own format string because alignment and the alternate flag
        // cannot be passed as arguments.
        let res = match (f.align(), f.alternate(), f.precision()) {
            (None, false, None) => write!(out, concat!("{:w$", $tr, "}"), value, w = width),
            (None, true, None) => write!(out, concat!("{:#w$", $tr, "}"), value, w = width),
            (None, false, Some(p)) => {
                write!(out, concat!("{:w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (None, true, Some(p)) => {
                write!(out, concat!("{:#w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Left), false, None) => {
                write!(out, concat!("{:<w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Left), true, None) => {
                write!(out, concat!("{:<#w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Left), false, Some(p)) => {
                write!(out, concat!("{:<w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Left), true, Some(p)) => {
                write!(out, concat!("{:<#w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Right), false, None) => {
                write!(out, concat!("{:>w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Right), true, None) => {
                write!(out, concat!("{:>#w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Right), false, Some(p)) => {
                write!(out, concat!("{:>w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Right), true, Some(p)) => {
                write!(out, concat!("{:>#w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Center), false, None) => {
                write!(out, concat!("{:^w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Center), true, None) => {
                write!(out, concat!("{:^#w$", $tr, "}"), value, w = width)
            }
            (Some(fmt::Alignment::Center), false, Some(p)) => {
                write!(out, concat!("{:^w$.p$", $tr, "}"), value, w = width, p = p)
            }
            (Some(fmt::Alignment::Center), true, Some(p)) => {
                write!(out, concat!("{:^#w$.p$", $tr, "}"), value, w = width, p = p)
            }
        };
        res.map(|_| out)
    }};
}

fn write_tagged(f: &mut Formatter<'_>, category: Category, rendered: &str) -> fmt::Result {
    let tag = category.tag();
    write!(f, "<{tag}>")?;
    write_escaped(f, rendered)?;
    write!(f, "</{tag}>")
}

impl<T: Display + ?Sized> Display for Redacted<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !is_log_redaction_enabled() {
            return Display::fmt(self.value, f);
        }
        let rendered = render_with_spec!(f, self.value, "")?;
        write_tagged(f, self.category, &rendered)
    }
}

impl<T: Debug + ?Sized> Debug for Redacted<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !is_log_redaction_enabled() {
            return Debug::fmt(self.value, f);
        }
        let rendered = render_with_spec!(f, self.value, "?")?;
        write_tagged(f, self.category, &rendered)
    }
}

impl<T: Display + ?Sized> Display for Unredacted<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self.value, f)
    }
}

impl<T: Debug + ?Sized> Debug for Unredacted<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.value, f)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    // Redaction is process-wide state and unit tests run in parallel, so every test that depends
    // on it holds this lock for its whole body, and restores the previous state when done even if
    // an assertion fails part way through.
    static REDACTION_LOCK: Mutex<()> = Mutex::new(());

    pub(crate) struct ScopedLogRedaction {
        previous: bool,
        _guard: MutexGuard<'static, ()>,
    }

    impl ScopedLogRedaction {
        pub(crate) fn new(enable: bool) -> Self {
            let guard = REDACTION_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let previous = is_log_redaction_enabled();
            LOG_REDACTION_ENABLED.store(enable, Ordering::Relaxed);
            Self {
                previous,
                _guard: guard,
            }
        }

        // Changes the state while keeping the lock, for a test that needs both states.
        pub(crate) fn set(&self, enable: bool) {
            LOG_REDACTION_ENABLED.store(enable, Ordering::Relaxed);
        }
    }

    impl Drop for ScopedLogRedaction {
        fn drop(&mut self) {
            LOG_REDACTION_ENABLED.store(self.previous, Ordering::Relaxed);
        }
    }

    // The tool that consumes these tags is line-oriented. A value carrying a newline puts the
    // opening tag and the closing tag on different lines, and the tool then emits the value in
    // clear text.
    const MULTILINE_BODY: &str = "<html><head>\n<title>301</title></head>\r\n</html>";

    // A value carrying a tag sequence would end its span early and leave the rest of itself
    // outside any tag.
    const INJECTED_TAG: &str = "</ud>secret<ud>";

    const LIST_ADDRESSES: [&str; 2] = ["10.0.0.1:11210", "10.0.0.2:11210"];

    #[test]
    fn annotations_are_inert_while_redaction_is_disabled() {
        let _r = ScopedLogRedaction::new(false);

        assert_eq!(format!("key={}", user_data("my_key")), "key=my_key");
        assert_eq!(
            format!("bucket={}", metadata("my_bucket")),
            "bucket=my_bucket"
        );
        assert_eq!(
            format!("host={}", system_data("127.0.0.1")),
            "host=127.0.0.1"
        );
    }

    #[test]
    fn annotations_wrap_values_while_redaction_is_enabled() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            format!("key={}", user_data("my_key")),
            "key=<ud>my_key</ud>"
        );
        assert_eq!(
            format!("bucket={}", metadata("my_bucket")),
            "bucket=<md>my_bucket</md>"
        );
        assert_eq!(
            format!("host={}", system_data("127.0.0.1")),
            "host=<sd>127.0.0.1</sd>"
        );
    }

    #[test]
    fn debug_formatting_is_tagged_too() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            format!("{:?}", metadata(&Some("travel-sample"))),
            r#"<md>Some("travel-sample")</md>"#
        );
    }

    #[test]
    fn a_tagged_value_never_spans_log_lines() {
        let _r = ScopedLogRedaction::new(true);

        let tagged = format!("body={}", user_data(MULTILINE_BODY));
        assert_eq!(
            tagged,
            r"body=<ud><html><head>\n<title>301</title></head>\r\n</html></ud>"
        );
        assert!(!tagged.contains('\n'));
        assert!(!tagged.contains('\r'));

        assert!(!format!("{}", metadata(MULTILINE_BODY)).contains('\n'));
        assert!(!format!("{}", system_data(MULTILINE_BODY)).contains('\n'));
    }

    #[test]
    fn pretty_debug_output_is_flattened_inside_a_tag() {
        let _r = ScopedLogRedaction::new(true);

        let tagged = format!("{:#?}", user_data(&vec!["a", "b"]));
        assert_eq!(tagged, r#"<ud>[\n    "a",\n    "b",\n]</ud>"#);
    }

    #[test]
    fn every_entry_of_a_tagged_list_is_escaped() {
        let _r = ScopedLogRedaction::new(true);

        let values = ["one\ntwo", "three"];
        assert_eq!(
            system_data_list(&values).to_string(),
            r"<sd>one\ntwo</sd>, <sd>three</sd>"
        );
    }

    #[test]
    fn a_value_with_newlines_is_untouched_while_redaction_is_disabled() {
        let _r = ScopedLogRedaction::new(false);

        assert_eq!(
            format!("body={}", user_data(MULTILINE_BODY)),
            format!("body={MULTILINE_BODY}")
        );
    }

    #[test]
    fn the_exclusion_markers_never_escape_their_value() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(format!("{}", not_redacted(MULTILINE_BODY)), MULTILINE_BODY);
        assert_eq!(format!("{}", not_sensitive(MULTILINE_BODY)), MULTILINE_BODY);
    }

    #[test]
    fn exclusion_markers_never_change_what_is_printed() {
        let r = ScopedLogRedaction::new(false);
        let host = "10.0.0.1";

        assert_eq!(format!("host={}", not_sensitive(host)), "host=10.0.0.1");
        assert_eq!(format!("host={}", not_redacted(host)), "host=10.0.0.1");

        r.set(true);
        assert_eq!(format!("host={}", not_sensitive(host)), "host=10.0.0.1");
        assert_eq!(format!("host={}", not_redacted(host)), "host=10.0.0.1");
        assert_eq!(format!("{:?}", not_redacted(&Some(1))), "Some(1)");
    }

    #[test]
    fn a_tagged_value_cannot_close_its_own_span() {
        let _r = ScopedLogRedaction::new(true);

        let tagged = format!("key={}", user_data(INJECTED_TAG));
        assert_eq!(tagged, r"key=<ud>\u003c/ud>secret\u003cud></ud>");

        // The span has to be the only one, and it has to close exactly once, at the end.
        assert_eq!(tagged.find("</ud>"), Some(tagged.len() - 5));
        assert_eq!(tagged.find("<ud>"), Some("key=".len()));
    }

    #[test]
    fn markup_that_is_not_a_tag_sequence_is_left_alone() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            format!("{}", user_data("<html><body>")),
            "<ud><html><body></ud>"
        );
    }

    #[test]
    fn tag_injection_is_escaped_in_every_category_and_in_a_list() {
        let _r = ScopedLogRedaction::new(true);

        let tagged = format!("{}", metadata(INJECTED_TAG));
        assert_eq!(tagged.find("</md>"), Some(tagged.len() - 5));
        assert_eq!(
            system_data_list(&[INJECTED_TAG]).to_string(),
            r"<sd>\u003c/ud>secret\u003cud></sd>"
        );
    }

    #[test]
    fn escaping_keeps_multibyte_characters_intact() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            format!("{}", user_data("zażółć\n</sd>")),
            r"<ud>zażółć\n\u003c/sd></ud>"
        );
    }

    #[test]
    fn format_specifications_apply_inside_the_tag() {
        let r = ScopedLogRedaction::new(false);

        assert_eq!(format!("[{:>8}]", metadata("bucket")), "[  bucket]");
        assert_eq!(format!("[{:w$}]", metadata("ab"), w = 6), "[ab    ]");

        r.set(true);
        assert_eq!(
            format!("[{:>8}]", metadata("bucket")),
            "[<md>  bucket</md>]"
        );
        assert_eq!(
            format!("[{:w$}]", metadata("ab"), w = 6),
            "[<md>ab    </md>]"
        );
        assert_eq!(format!("{:^6}", metadata("ab")), "<md>  ab  </md>");
        assert_eq!(format!("{:.2}", system_data(&1.23456)), "<sd>1.23</sd>");
        // With no alignment given, the value's own default applies: numbers pad on the left.
        assert_eq!(format!("{:5}", system_data(&42)), "<sd>   42</sd>");
    }

    #[test]
    fn annotations_work_for_non_string_values() {
        let _r = ScopedLogRedaction::new(true);

        let host = String::from("10.0.0.1");
        assert_eq!(format!("{}", system_data(&11210)), "<sd>11210</sd>");
        assert_eq!(format!("{}", system_data(&host)), "<sd>10.0.0.1</sd>");
        assert_eq!(format!("{}", metadata(host.as_str())), "<md>10.0.0.1</md>");
    }

    #[test]
    fn annotations_may_be_mixed_in_a_single_statement() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            format!(
                "[{}/{}] <{}> key={}",
                "client-id",
                metadata("travel-sample"),
                system_data("10.0.0.1:11210"),
                user_data("airline_10")
            ),
            "[client-id/<md>travel-sample</md>] <<sd>10.0.0.1:11210</sd>> key=<ud>airline_10</ud>"
        );
    }

    #[test]
    fn a_list_is_inert_while_redaction_is_disabled() {
        let _r = ScopedLogRedaction::new(false);

        assert_eq!(
            system_data_list(&LIST_ADDRESSES).to_string(),
            "10.0.0.1:11210, 10.0.0.2:11210"
        );
        assert_eq!(
            system_data_list(&LIST_ADDRESSES).quoted().to_string(),
            r#""10.0.0.1:11210", "10.0.0.2:11210""#
        );
    }

    #[test]
    fn a_list_is_tagged_one_entry_at_a_time() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            system_data_list(&LIST_ADDRESSES).to_string(),
            "<sd>10.0.0.1:11210</sd>, <sd>10.0.0.2:11210</sd>"
        );

        let owned: Vec<String> = LIST_ADDRESSES.iter().map(|a| a.to_string()).collect();
        assert_eq!(
            system_data_list(&owned).to_string(),
            "<sd>10.0.0.1:11210</sd>, <sd>10.0.0.2:11210</sd>"
        );
    }

    #[test]
    fn a_list_tag_sits_inside_the_quotes_an_entry_renders() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            system_data_list(&LIST_ADDRESSES).quoted().to_string(),
            r#""<sd>10.0.0.1:11210</sd>", "<sd>10.0.0.2:11210</sd>""#
        );
    }

    #[test]
    fn a_list_uses_the_separator_the_caller_gives() {
        let _r = ScopedLogRedaction::new(true);

        assert_eq!(
            system_data_list(&LIST_ADDRESSES)
                .quoted()
                .separator(",")
                .to_string(),
            r#""<sd>10.0.0.1:11210</sd>","<sd>10.0.0.2:11210</sd>""#
        );
    }

    #[test]
    fn a_list_accepts_an_iterator() {
        let _r = ScopedLogRedaction::new(true);

        let mut endpoints = std::collections::BTreeMap::new();
        endpoints.insert("kvep-a-8091", ());
        endpoints.insert("kvep-b-8091", ());
        assert_eq!(
            system_data_list(endpoints.keys()).to_string(),
            "<sd>kvep-a-8091</sd>, <sd>kvep-b-8091</sd>"
        );
    }

    #[test]
    fn an_empty_list_renders_nothing() {
        let r = ScopedLogRedaction::new(false);
        let empty: Vec<String> = vec![];

        assert!(system_data_list(&empty).to_string().is_empty());

        r.set(true);
        assert!(system_data_list(&empty).to_string().is_empty());
        assert!(system_data_list(&empty).quoted().to_string().is_empty());
    }

    #[test]
    fn a_string_built_while_redaction_was_on_keeps_its_tags() {
        // A message composed with format! ahead of the log statement is fixed when it is built,
        // and does not follow later changes to the redaction setting. This is why redaction must
        // be enabled before connecting.
        let r = ScopedLogRedaction::new(true);
        let tagged = format!("[{}]", metadata("travel-sample"));

        r.set(false);
        assert_eq!(tagged, "[<md>travel-sample</md>]");

        let untagged = format!("[{}]", metadata("travel-sample"));
        r.set(true);
        assert_eq!(untagged, "[travel-sample]");
    }

    #[test]
    fn enabling_redaction_is_observable() {
        let r = ScopedLogRedaction::new(false);
        assert!(!is_log_redaction_enabled());

        set_log_redaction(true);
        assert!(is_log_redaction_enabled());

        set_log_redaction(false);
        assert!(!is_log_redaction_enabled());
        drop(r);
    }
}
