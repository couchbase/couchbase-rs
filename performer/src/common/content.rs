use crate::errors::error::{Error, Result};
use crate::proto::protocol::shared;
use crate::proto::protocol::shared::content::Content;
use crate::proto::protocol::shared::content_as::As;
use crate::proto::protocol::shared::ContentTypes;
use couchbase::results::kv_results::GetResult;
use couchbase::transcoding::{json, raw_binary, raw_json, raw_string};
use serde::de::DeserializeOwned;
use serde_json::Value;

pub enum SdkContent {
    PassthroughString(String),
    Json(Value),
    ByteArray(Vec<u8>),
    None,
}

// Anything with Raw will use the `_raw` methods
pub enum EncodedContent {
    Raw(Vec<u8>, u32),
    PassthroughString(String),
    Json(Value),
    ByteArray(Vec<u8>),
    None,
}

pub enum Transcoder {
    Json,
    RawBinary,
    RawJson,
    RawString,
}

pub trait ContentDecoder {
    fn content_as<V: DeserializeOwned>(&self) -> couchbase::error::Result<V>;
    fn content_as_raw(&self) -> (&[u8], u32);
}

impl ContentDecoder for GetResult {
    fn content_as<V: DeserializeOwned>(&self) -> couchbase::error::Result<V> {
        self.content_as::<V>()
    }

    fn content_as_raw(&self) -> (&[u8], u32) {
        self.content_as_raw()
    }
}

pub fn parse_content_as<R: ContentDecoder>(
    content_as: &As,
    transcoder: &Option<Transcoder>,
    result: &R,
) -> Result<ContentTypes> {
    let content = match transcoder {
        Some(Transcoder::Json) => decode_with_json_transcoder(content_as, result)?,
        Some(Transcoder::RawBinary) => decode_with_raw_binary_transcoder(content_as, result)?,
        Some(Transcoder::RawJson) => decode_with_raw_json_transcoder(content_as, result)?,
        Some(Transcoder::RawString) => decode_with_raw_string_transcoder(content_as, result)?,
        None => decode_with_no_transcoder(content_as, result)?,
    };

    Ok(ContentTypes {
        content: Some(content),
    })
}

fn decode_with_json_transcoder<R: ContentDecoder>(
    content_as: &As,
    result: &R,
) -> Result<shared::content_types::Content> {
    let (bytes, flags) = result.content_as_raw();

    match content_as {
        As::AsString(_) => {
            let res: String = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsString failed"))?;
            Ok(shared::content_types::Content::ContentAsString(res))
        }
        As::AsByteArray(_) => {
            let res: Vec<u8> = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsByteArray failed"))?;
            Ok(shared::content_types::Content::ContentAsBytes(res))
        }
        As::AsJsonObject(_) | As::AsJsonArray(_) => {
            let res: Value = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsJsonObject failed"))?;
            let bytes = serde_json::to_vec(&res).map_err(|_| {
                Error::internal("Get result from AsJsonObject serialization failed")
            })?;
            Ok(shared::content_types::Content::ContentAsBytes(bytes))
        }
        As::AsBoolean(_) => {
            let res: bool = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsBoolean failed"))?;
            Ok(shared::content_types::Content::ContentAsBool(res))
        }
        As::AsInteger(_) => {
            let res: i64 = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsInteger failed"))?;
            Ok(shared::content_types::Content::ContentAsInt64(res))
        }
        As::AsFloatingPoint(_) => {
            let res: f64 = json::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsFloatingPoint failed"))?;
            Ok(shared::content_types::Content::ContentAsDouble(res))
        }
    }
}

fn decode_with_raw_binary_transcoder<R: ContentDecoder>(
    content_as: &As,
    result: &R,
) -> Result<shared::content_types::Content> {
    let (bytes, flags) = result.content_as_raw();

    match content_as {
        As::AsByteArray(_) => {
            let res: &[u8] = raw_binary::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsByteArray failed"))?;
            Ok(shared::content_types::Content::ContentAsBytes(res.to_vec()))
        }
        _ => Err(Error::invalid_argument(
            "RawBinary transcoder -- Unsupported content type combination",
        )),
    }
}

fn decode_with_raw_json_transcoder<R: ContentDecoder>(
    content_as: &As,
    result: &R,
) -> Result<shared::content_types::Content> {
    let (bytes, flags) = result.content_as_raw();

    match content_as {
        As::AsByteArray(_) => {
            let res = raw_json::decode(bytes, flags)
                .map_err(|_| Error::internal("RawJson: Get result from AsByteArray failed"))?;
            Ok(shared::content_types::Content::ContentAsBytes(res.to_vec()))
        }
        As::AsString(_) => {
            let res = String::from_utf8(
                raw_json::decode(bytes, flags)
                    .map_err(|_| Error::internal("RawJson: Get result from AsString failed"))?
                    .to_vec(),
            )
            .map_err(|_| Error::internal("RawJson: Get result from AsString UTF-8 failed"))?;
            Ok(shared::content_types::Content::ContentAsString(res))
        }
        _ => Err(Error::invalid_argument(
            "RawJson transcoder -- Unsupported content type combination",
        )),
    }
}

fn decode_with_raw_string_transcoder<R: ContentDecoder>(
    content_as: &As,
    result: &R,
) -> Result<shared::content_types::Content> {
    let (bytes, flags) = result.content_as_raw();

    match content_as {
        As::AsString(_) => {
            let res = raw_string::decode(bytes, flags)
                .map_err(|_| Error::internal("Get result from AsString failed"))?
                .to_string();
            Ok(shared::content_types::Content::ContentAsString(res))
        }
        _ => Err(Error::invalid_argument(
            "RawString transcoder -- Unsupported content type combination",
        )),
    }
}

fn decode_with_no_transcoder<R: ContentDecoder>(
    content_as: &As,
    result: &R,
) -> Result<shared::content_types::Content> {
    match content_as {
        As::AsString(_) => {
            let res = result
                .content_as::<String>()
                .map_err(|_| Error::internal("Get result from AsString failed"))?;
            Ok(shared::content_types::Content::ContentAsString(res))
        }
        As::AsByteArray(_) => {
            let res = result
                .content_as::<Vec<u8>>()
                .map_err(|_| Error::internal("Get result from AsByteArray failed"))?;
            Ok(shared::content_types::Content::ContentAsBytes(res))
        }
        As::AsJsonObject(_) | As::AsJsonArray(_) => {
            let json = result
                .content_as::<Value>()
                .map_err(|_| Error::internal("Get result from AsJsonObject failed"))?;
            let bytes = serde_json::to_vec(&json).map_err(|_| {
                Error::internal("Get result from AsJsonObject serialization failed")
            })?;
            Ok(shared::content_types::Content::ContentAsBytes(bytes))
        }
        As::AsBoolean(_) => {
            let res = result
                .content_as::<bool>()
                .map_err(|_| Error::internal("Get result from AsBoolean failed"))?;
            Ok(shared::content_types::Content::ContentAsBool(res))
        }
        As::AsInteger(_) => {
            let res = result
                .content_as::<i64>()
                .map_err(|_| Error::internal("Get result from AsInteger failed"))?;
            Ok(shared::content_types::Content::ContentAsInt64(res))
        }
        As::AsFloatingPoint(_) => {
            let res = result
                .content_as::<f64>()
                .map_err(|_| Error::internal("Get result from AsFloatingPoint failed"))?;
            Ok(shared::content_types::Content::ContentAsDouble(res))
        }
    }
}

pub fn encode_content(
    content: &SdkContent,
    transcoder: &Option<Transcoder>,
) -> Result<EncodedContent> {
    if let Some(transcoder) = transcoder {
        let (bytes, flags) = match (transcoder, content) {
            (Transcoder::Json, SdkContent::PassthroughString(s)) => {
                json::encode(s).map_err(|e| Error::internal(e.to_string()))
            }
            (Transcoder::Json, SdkContent::Json(j)) => {
                json::encode(j).map_err(|e| Error::internal(e.to_string()))
            }
            (Transcoder::Json, SdkContent::None) => {
                json::encode(None::<()>).map_err(|e| Error::internal(e.to_string()))
            }
            (Transcoder::RawBinary, SdkContent::ByteArray(b)) => {
                let (slice, flags) =
                    raw_binary::encode(b).map_err(|e| Error::internal(e.to_string()))?;
                Ok((slice.to_vec(), flags))
            }
            (Transcoder::RawJson, SdkContent::ByteArray(b)) => {
                let (slice, flags) =
                    raw_json::encode(b).map_err(|e| Error::internal(e.to_string()))?;
                Ok((slice.to_vec(), flags))
            }
            (Transcoder::RawJson, SdkContent::PassthroughString(s)) => {
                let (slice, flags) =
                    raw_json::encode(s).map_err(|e| Error::internal(e.to_string()))?;
                Ok((slice.to_vec(), flags))
            }
            (Transcoder::RawString, SdkContent::PassthroughString(s)) => {
                let (slice, flags) =
                    raw_string::encode(s).map_err(|e| Error::internal(e.to_string()))?;
                Ok((slice.to_vec(), flags))
            }
            _ => {
                return Err(Error::invalid_argument(
                    "Unsupported transcoder/content combination",
                ));
            }
        }?;
        Ok(EncodedContent::Raw(bytes, flags))
    } else {
        match content {
            SdkContent::PassthroughString(s) => Ok(EncodedContent::PassthroughString(s.clone())),
            SdkContent::Json(j) => Ok(EncodedContent::Json(j.clone())),
            SdkContent::None => Ok(EncodedContent::None),
            SdkContent::ByteArray(b) => Ok(EncodedContent::ByteArray(b.clone())),
        }
    }
}

pub fn content_from_shared(content: Content) -> Result<SdkContent> {
    match content {
        Content::PassthroughString(s) => Ok(SdkContent::PassthroughString(s)),
        Content::ConvertToJson(b) => {
            let json: Value =
                serde_json::from_slice(&b).map_err(|_| Error::invalid_argument("Invalid JSON"))?;
            Ok(SdkContent::Json(json))
        }
        Content::ByteArray(b) => Ok(SdkContent::ByteArray(b)),
        Content::Null(_) => Ok(SdkContent::None),
    }
}

pub fn transcoder_from_shared(transcoder: &shared::transcoder::Transcoder) -> Result<Transcoder> {
    match &transcoder {
        shared::transcoder::Transcoder::Json(_) => Ok(Transcoder::Json),
        shared::transcoder::Transcoder::RawBinary(_) => Ok(Transcoder::RawBinary),
        shared::transcoder::Transcoder::RawJson(_) => Ok(Transcoder::RawJson),
        shared::transcoder::Transcoder::RawString(_) => Ok(Transcoder::RawString),
        _ => Err(Error::unimplemented("Unsupported transcoder type")),
    }
}
