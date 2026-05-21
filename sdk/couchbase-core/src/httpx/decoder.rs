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

use crate::httpx::error;
use crate::httpx::error::Result as HttpxResult;
use crate::httpx::scanner::{ScanState, Scanner};
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use tokio_stream::StreamExt;

pub type DecoderStream = dyn Stream<Item = error::Result<Bytes>> + Send + Unpin;

pub struct Decoder {
    r: Pin<Box<DecoderStream>>,
    buf: Vec<u8>,
    scanp: usize,
    scanned: usize,
    scan: Scanner,
    err: Option<error::Error>,
    token_state: TokenState,
    token_stack: Vec<TokenState>,
}

impl Decoder {
    pub fn new<R>(r: R) -> Self
    where
        R: Stream<Item = error::Result<Bytes>> + Send + 'static + Unpin,
    {
        Decoder {
            r: Box::pin(r),
            buf: Vec::new(),
            scanp: 0,
            scanned: 0,
            scan: Scanner::new(),
            err: None,
            token_state: TokenState::TopValue,
            token_stack: Vec::new(),
        }
    }

    pub async fn decode(&mut self) -> HttpxResult<Vec<u8>> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        self.token_prepare_for_decode().await?;

        if !self.token_value_allowed() {
            return Err(error::Error::new_message_error("not at beginning of value"));
        }

        let n = self.read_value().await?;
        let val = self.buf[self.scanp..self.scanp + n].trim_ascii().to_vec();
        self.scanp += n;

        self.token_value_end();

        Ok(val)
    }

    fn buffered(&self) -> &[u8] {
        &self.buf[self.scanp..]
    }

    async fn read_value(&mut self) -> HttpxResult<usize> {
        self.scan.reset();
        let mut scanp = self.scanp;
        let mut res: Option<HttpxResult<()>> = None;

        loop {
            while scanp < self.buf.len() {
                let c = self.buf[scanp];
                self.scan.incr_bytes(1);
                match self.scan.step(c) {
                    ScanState::End => {
                        self.scan.incr_bytes(-1);
                        return Ok(scanp - self.scanp);
                    }
                    ScanState::EndObject | ScanState::EndArray
                        if self.scan.step(b' ') == ScanState::End =>
                    {
                        scanp += 1;
                        return Ok(scanp - self.scanp);
                    }
                    ScanState::Error => {
                        let scan_err = self.scan.err().expect("scan state error but no error set");
                        self.err = Some(scan_err.clone());
                        return Err(scan_err.clone());
                    }
                    _ => {}
                }
                scanp += 1;
            }

            // Did the last read have an error?
            // Delayed until now to allow buffer scan.
            if let Some(Err(e)) = res {
                self.err = Some(e.clone());
                return Err(e);
            }

            let n = scanp - self.scanp;
            res = self.refill().await;
            scanp = self.scanp + n;

            if res.is_none() {
                if self.scan.step(b' ') == ScanState::End {
                    return Ok(scanp - self.scanp);
                }

                if self.buf.iter().any(|&b| !b.is_ascii_whitespace()) {
                    self.err = Some(error::Error::new_message_error("unexpected EOF"));
                }

                return match self.err {
                    Some(ref e) => Err(e.clone()),
                    None => Ok(scanp - self.scanp),
                };
            }
        }
    }

    async fn refill(&mut self) -> Option<HttpxResult<()>> {
        // Make room to read more into the buffer.
        // First slide down data already consumed.
        if self.scanp > 0 {
            self.scanned += self.scanp;
            let n = self.buf.len() - self.scanp;
            self.buf.copy_within(self.scanp.., 0);
            self.buf.truncate(n);
            self.scanp = 0;
        }

        if let Some(r) = self.r.next().await {
            return match r {
                Ok(buf) => {
                    self.buf.extend_from_slice(&buf[..]);
                    Some(Ok(()))
                }
                Err(e) => Some(Err(e)),
            };
        };

        None
    }

    async fn token_prepare_for_decode(&mut self) -> HttpxResult<()> {
        match self.token_state {
            TokenState::ArrayComma => {
                let c = match self.peek().await {
                    Some(Ok(c)) => c,
                    Some(Err(e)) => return Err(e),
                    None => return Err(error::Error::new_message_error("unexpected EOF")),
                };
                if c != b',' {
                    return Err(error::Error::new_message_error(
                        "expected comma after array element",
                    ));
                }
                self.scanp += 1;
                self.token_state = TokenState::ArrayValue;
            }
            TokenState::ObjectColon => {
                let c = match self.peek().await {
                    Some(Ok(c)) => c,
                    Some(Err(e)) => return Err(e),
                    None => return Err(error::Error::new_message_error("unexpected EOF")),
                };
                if c != b':' {
                    return Err(error::Error::new_message_error(
                        "expected colon after object key",
                    ));
                }
                self.scanp += 1;
                self.token_state = TokenState::ObjectValue;
            }
            _ => {}
        }
        Ok(())
    }

    fn token_value_allowed(&self) -> bool {
        matches!(
            self.token_state,
            TokenState::TopValue
                | TokenState::ArrayStart
                | TokenState::ArrayValue
                | TokenState::ObjectValue
        )
    }

    fn token_value_end(&mut self) {
        match self.token_state {
            TokenState::ArrayStart | TokenState::ArrayValue => {
                self.token_state = TokenState::ArrayComma;
            }
            TokenState::ObjectValue => {
                self.token_state = TokenState::ObjectComma;
            }
            _ => {}
        }
    }

    async fn peek(&mut self) -> Option<HttpxResult<u8>> {
        let mut res = None;
        loop {
            for i in self.scanp..self.buf.len() {
                let c = self.buf[i];
                if c.is_ascii_whitespace() {
                    continue;
                }
                self.scanp = i;
                return Some(Ok(c));
            }
            if let Some(r) = res {
                match r {
                    Ok(_) => {}
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }

            res = match self.refill().await {
                Some(r) => Some(r),
                None => {
                    return None;
                }
            };
        }
    }

    pub async fn token(&mut self) -> HttpxResult<Token> {
        loop {
            let c = match self.peek().await {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(e),
                None => return Err(error::Error::new_message_error("unexpected EOF")),
            };
            match c {
                b'[' => {
                    if !self.token_value_allowed() {
                        return self.token_error(c);
                    }
                    self.scanp += 1;
                    self.token_stack.push(self.token_state);
                    self.token_state = TokenState::ArrayStart;
                    return Ok(Token::Delim('['));
                }
                b']' => {
                    if self.token_state != TokenState::ArrayStart
                        && self.token_state != TokenState::ArrayComma
                    {
                        return self.token_error(c);
                    }
                    self.scanp += 1;
                    self.token_state = self.token_stack.pop().unwrap();
                    self.token_value_end();
                    return Ok(Token::Delim(']'));
                }
                b'{' => {
                    if !self.token_value_allowed() {
                        return self.token_error(c);
                    }
                    self.scanp += 1;
                    self.token_stack.push(self.token_state);
                    self.token_state = TokenState::ObjectStart;
                    return Ok(Token::Delim('{'));
                }
                b'}' => {
                    if self.token_state != TokenState::ObjectStart
                        && self.token_state != TokenState::ObjectComma
                    {
                        return self.token_error(c);
                    }
                    self.scanp += 1;
                    self.token_state = self.token_stack.pop().unwrap();
                    self.token_value_end();
                    return Ok(Token::Delim('}'));
                }
                b':' => {
                    if self.token_state != TokenState::ObjectColon {
                        return self.token_error(c);
                    }
                    self.scanp += 1;
                    self.token_state = TokenState::ObjectValue;
                    continue;
                }
                b',' => {
                    if self.token_state == TokenState::ArrayComma {
                        self.scanp += 1;
                        self.token_state = TokenState::ArrayValue;
                        continue;
                    }
                    if self.token_state == TokenState::ObjectComma {
                        self.scanp += 1;
                        self.token_state = TokenState::ObjectKey;
                        continue;
                    }
                    return self.token_error(c);
                }
                b'"' => {
                    if self.token_state == TokenState::ObjectStart
                        || self.token_state == TokenState::ObjectKey
                    {
                        let old = self.token_state;
                        self.token_state = TokenState::TopValue;
                        let decoded = self.decode().await?;
                        let x = serde_json::from_slice(&decoded)
                            .map_err(|e| error::Error::new_message_error(format!("{e}")))?;
                        self.token_state = old;
                        self.token_state = TokenState::ObjectColon;
                        return Ok(Token::String(x));
                    }

                    if !self.token_value_allowed() {
                        return self.token_error(c);
                    }

                    let decoded = self.decode().await?;
                    return Ok(Token::Value(decoded));
                }
                _ => {
                    if !self.token_value_allowed() {
                        return self.token_error(c);
                    }

                    let decoded = self.decode().await?;
                    return Ok(Token::Value(decoded));
                }
            }
        }
    }

    fn token_error(&self, c: u8) -> HttpxResult<Token> {
        let context = match self.token_state {
            TokenState::TopValue
            | TokenState::ArrayStart
            | TokenState::ArrayValue
            | TokenState::ObjectValue => " looking for beginning of value",
            TokenState::ArrayComma => " after array element",
            TokenState::ObjectKey => " looking for beginning of object key string",
            TokenState::ObjectColon => " after object key",
            TokenState::ObjectComma => " after object key:value pair",
            _ => "",
        };
        Err(error::Error::new_message_error(format!(
            "invalid character {}{}",
            Scanner::quote_char(c),
            context
        )))
    }

    pub async fn more(&mut self) -> bool {
        let c = self.peek().await;
        match c {
            Some(Ok(c)) => c != b']' && c != b'}',
            Some(Err(_)) => false,
            None => false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TokenState {
    TopValue,
    ArrayStart,
    ArrayValue,
    ArrayComma,
    ObjectStart,
    ObjectKey,
    ObjectColon,
    ObjectValue,
    ObjectComma,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    Delim(char),
    String(String),
    Value(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    struct TestStream {
        data: Vec<Bytes>,
    }

    impl TestStream {
        fn new(data: Vec<Bytes>) -> Self {
            TestStream { data }
        }
    }

    impl Unpin for TestStream {}

    impl Stream for TestStream {
        type Item = error::Result<Bytes>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if self.data.is_empty() {
                std::task::Poll::Ready(None)
            } else {
                std::task::Poll::Ready(Some(Ok(self.data.remove(0))))
            }
        }
    }

    #[tokio::test]
    async fn test_decode_object() {
        let data = vec![Bytes::from_static(b"{\"key\":\"value\"}")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!({"key": "value"}));
    }

    #[tokio::test]
    async fn test_decode_array() {
        let data = vec![Bytes::from_static(b"[1, 2, 3]")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[tokio::test]
    async fn test_decode_string() {
        let data = vec![Bytes::from_static(b"\"hello\"")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_decode_number() {
        let data = vec![Bytes::from_static(b"123")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!(123));
    }

    #[tokio::test]
    async fn test_decode_boolean() {
        let data = vec![Bytes::from_static(b"true")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!(true));
    }

    #[tokio::test]
    async fn test_decode_null() {
        let data = vec![Bytes::from_static(b"null")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let result = decoder.decode().await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(result, serde_json::json!(null));
    }
    #[tokio::test]
    async fn test_token_object_start() {
        let data = vec![Bytes::from_static(b"{\"key\":\"value\"}")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim('{'));
    }

    #[tokio::test]
    async fn test_token_object_end() {
        let data = vec![Bytes::from_static(
            b"{\"key\":\"value\", \"key2\":\"value2\"}",
        )];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        // Read the start of the object
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim('{'));
        // Read the key
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::String("key".to_string()));
        // Read the value
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(Vec::from(r#""value""#)));
        // Read the key2
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::String("key2".to_string()));
        // Read the value2
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(Vec::from(r#""value2""#)));
        // Read the end of the object
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim('}'));
    }

    #[tokio::test]
    async fn test_token_array_start() {
        let data = vec![Bytes::from_static(b"[1, 2, 3]")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim('['));
    }

    #[tokio::test]
    async fn test_token_array_end() {
        let data = vec![Bytes::from_static(b"[1, 2, 3]")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        // Read the start of the array
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim('['));
        // Read the first value
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"1".to_vec()));
        // Read the second value
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"2".to_vec()));
        // Read the third value
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"3".to_vec()));
        // Read the end of the array
        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Delim(']'));
    }

    #[tokio::test]
    async fn test_token_string() {
        let data = vec![Bytes::from_static(b"\"hello\"")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(Vec::from(r#""hello""#)));
    }

    #[tokio::test]
    async fn test_token_number() {
        let data = vec![Bytes::from_static(b"123")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"123".to_vec()));
    }

    #[tokio::test]
    async fn test_token_boolean() {
        let data = vec![Bytes::from_static(b"true")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"true".to_vec()));
    }

    #[tokio::test]
    async fn test_token_null() {
        let data = vec![Bytes::from_static(b"null")];
        let stream = TestStream::new(data);
        let mut decoder = Decoder::new(stream);

        let token = decoder.token().await.unwrap();
        assert_eq!(token, Token::Value(b"null".to_vec()));
    }
}
