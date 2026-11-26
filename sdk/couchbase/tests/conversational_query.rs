/*
 *
 *  * Copyright (c) 2026 Couchbase, Inc.
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

use crate::common::test_config::run_test;
use couchbase::collection::Collection;
use couchbase::conversational_query::ConversationalQueryChatHandle;
use couchbase::error::ErrorKind;
use couchbase::options::conversational_query_options::{
    ConversationalAttachChatOptions, ConversationalBeginChatOptions, ConversationalModelCredential,
};
use std::ops::Deref;

mod common;

fn default_collection(
    cluster: &common::test_cluster::TestCluster,
    bucket: &common::test_bucket::TestBucket,
) -> Collection {
    bucket
        .scope(cluster.default_scope())
        .collection(cluster.default_collection())
        .deref()
        .clone()
}

#[test]
fn test_conversational_query_requires_keyspaces() {
    run_test(async |cluster, _bucket| {
        let cq = cluster.conversational_query();

        let err = cq.query("hello", &[], None).await.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidArgument(_)), "{err}");

        let err = cq.begin_chat(&[], None).await.unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidArgument(_)), "{err}");
    })
}

#[test]
fn test_conversational_query_attach_chat_is_local() {
    run_test(async |cluster, _bucket| {
        let handle = ConversationalQueryChatHandle::from_json(
            r#"{"chatId": "chat-1", "owner": "no-such-node"}"#,
        )
        .unwrap();

        let chat = cluster
            .conversational_query()
            .attach_chat(
                &handle,
                ConversationalAttachChatOptions::new()
                    .credential(ConversationalModelCredential::api_key("sk-test")),
            )
            .unwrap();

        assert_eq!(handle, chat.handle().unwrap());
    })
}

#[test]
fn test_conversational_query_chat_unavailable_or_unsupported() {
    run_test(async |cluster, bucket| {
        let cq = cluster.conversational_query();
        let collection = default_collection(&cluster, &bucket);

        let begin = cq
            .begin_chat(&[collection], ConversationalBeginChatOptions::new())
            .await;
        let supported = begin.is_ok();
        if let Ok(chat) = begin {
            chat.end(None).await.unwrap();
        }

        for owner in ["null", r#""no-such-node""#] {
            let handle = ConversationalQueryChatHandle::from_json(&format!(
                r#"{{"chatId": "chat-1", "owner": {owner}}}"#
            ))
            .unwrap();
            let chat = cq.attach_chat(&handle, None).unwrap();

            let err = chat.ask("hello", None).await.unwrap_err();
            match err.kind() {
                ErrorKind::ConversationalChatCurrentlyUnavailable => {}
                // Feature detection takes precedence over an unknown owner.
                ErrorKind::FeatureNotAvailable(_) if !supported => {}
                _ => panic!("unexpected error: {err}"),
            }

            // The routing information is kept.
            assert_eq!(handle, chat.handle().unwrap());
        }
    })
}
