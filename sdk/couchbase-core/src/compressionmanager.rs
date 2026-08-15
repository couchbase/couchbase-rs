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

use snap::raw::Encoder;
use std::borrow::Cow;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::error;
use crate::error::ErrorKind;
use crate::memdx::datatype::DataTypeFlag;
use crate::options::agent::{CompressionConfig, CompressionMode};

pub(crate) trait Compressor: Send + Sync + Debug {
    fn new(compression_config: &CompressionConfig) -> Self;
    fn compress<'a>(
        &mut self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &'a [u8],
    ) -> error::Result<(Cow<'a, [u8]>, u8)>;
}

#[derive(Debug)]
pub(crate) struct CompressionManager<C> {
    _phantom: PhantomData<C>,
    compression_config: CompressionConfig,
}

impl<C> CompressionManager<C>
where
    C: Compressor,
{
    pub fn new(compression_config: CompressionConfig) -> Self {
        Self {
            _phantom: Default::default(),
            compression_config,
        }
    }

    pub fn compressor(&self) -> C {
        C::new(&self.compression_config)
    }
}

#[derive(Debug)]
pub(crate) struct StdCompressor {
    compression_enabled: bool,
    compression_min_size: usize,
    compression_min_ratio: f64,
}

impl Compressor for StdCompressor {
    fn new(compression_config: &CompressionConfig) -> Self {
        let (compression_enabled, compression_min_size, compression_min_ratio) =
            match compression_config.mode {
                CompressionMode::Enabled {
                    min_size,
                    min_ratio,
                } => (true, min_size, min_ratio),
                CompressionMode::Disabled => (false, 0, 0.0),
            };

        Self {
            compression_enabled,
            compression_min_size,
            compression_min_ratio,
        }
    }

    fn compress<'a>(
        &mut self,
        connection_supports_snappy: bool,
        datatype: DataTypeFlag,
        input: &'a [u8],
    ) -> error::Result<(Cow<'a, [u8]>, u8)> {
        if !connection_supports_snappy || !self.compression_enabled {
            return Ok((Cow::Borrowed(input), u8::from(datatype)));
        }

        let datatype = u8::from(datatype);

        // If the packet is already compressed then we don't want to compress it again.
        if datatype & u8::from(DataTypeFlag::Compressed) != 0 {
            return Ok((Cow::Borrowed(input), datatype));
        }

        let packet_size = input.len();

        // Only compress values that are large enough to be worthwhile.
        if packet_size <= self.compression_min_size {
            return Ok((Cow::Borrowed(input), datatype));
        }

        let mut encoder = Encoder::new();
        let compressed_value = encoder
            .compress_vec(input)
            .map_err(|e| ErrorKind::Compression { msg: e.to_string() })?;

        // Only return the compressed value if the ratio of compressed:original is small enough.
        if compressed_value.len() as f64 / packet_size as f64 > self.compression_min_ratio {
            return Ok((Cow::Borrowed(input), datatype));
        }

        Ok((
            Cow::Owned(compressed_value),
            datatype | u8::from(DataTypeFlag::Compressed),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn enabled_config(min_size: usize, min_ratio: f64) -> CompressionConfig {
        CompressionConfig::new(CompressionMode::Enabled {
            min_size,
            min_ratio,
        })
    }

    fn disabled_config() -> CompressionConfig {
        CompressionConfig::new(CompressionMode::Disabled)
    }

    #[test]
    fn disabled_compression_returns_input_unchanged() {
        let mut compressor = StdCompressor::new(&disabled_config());
        let input = b"hello world";

        let (output, dt) = compressor
            .compress(true, DataTypeFlag::Json, input)
            .unwrap();

        assert!(matches!(output, Cow::Borrowed(_)));
        assert_eq!(&*output, input.as_slice());
        assert_eq!(dt, u8::from(DataTypeFlag::Json));
    }

    #[test]
    fn connection_without_snappy_returns_input_unchanged() {
        let mut compressor = StdCompressor::new(&enabled_config(0, 1.0));
        let input = b"hello world";

        let (output, dt) = compressor
            .compress(false, DataTypeFlag::Json, input)
            .unwrap();

        assert!(matches!(output, Cow::Borrowed(_)));
        assert_eq!(&*output, input.as_slice());
    }

    #[test]
    fn already_compressed_returns_input_unchanged() {
        let mut compressor = StdCompressor::new(&enabled_config(0, 1.0));
        let input = b"already compressed data";

        let (output, dt) = compressor
            .compress(true, DataTypeFlag::Compressed, input)
            .unwrap();

        assert!(matches!(output, Cow::Borrowed(_)));
        assert_eq!(&*output, input.as_slice());
        assert_eq!(dt, u8::from(DataTypeFlag::Compressed));
    }

    #[test]
    fn input_below_min_size_returns_input_unchanged() {
        let mut compressor = StdCompressor::new(&enabled_config(1024, 1.0));
        let input = b"small";

        let (output, dt) = compressor
            .compress(true, DataTypeFlag::Json, input)
            .unwrap();

        assert!(matches!(output, Cow::Borrowed(_)));
        assert_eq!(&*output, input.as_slice());
        assert_eq!(dt, u8::from(DataTypeFlag::Json));
    }

    #[test]
    fn compressible_input_returns_owned_with_compressed_flag() {
        let mut compressor = StdCompressor::new(&enabled_config(0, 1.0));
        // Highly compressible: repeated bytes.
        let input = vec![b'a'; 4096];

        let (output, dt) = compressor
            .compress(true, DataTypeFlag::Json, &input)
            .unwrap();

        assert!(matches!(output, Cow::Owned(_)));
        assert!(output.len() < input.len());
        assert_eq!(
            dt,
            u8::from(DataTypeFlag::Json) | u8::from(DataTypeFlag::Compressed)
        );

        // Verify it round-trips through snappy.
        let decompressed = snap::raw::Decoder::new().decompress_vec(&output).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn poor_ratio_returns_input_unchanged() {
        // Set a very aggressive ratio that compressed output can't beat.
        let mut compressor = StdCompressor::new(&enabled_config(0, 0.01));
        let input = vec![b'a'; 256];

        let (output, dt) = compressor
            .compress(true, DataTypeFlag::Json, &input)
            .unwrap();

        assert!(matches!(output, Cow::Borrowed(_)));
        assert_eq!(&*output, input.as_slice());
        assert_eq!(dt, u8::from(DataTypeFlag::Json));
    }

    #[test]
    fn compression_manager_creates_compressor() {
        let manager = CompressionManager::<StdCompressor>::new(enabled_config(0, 1.0));
        let mut compressor = manager.compressor();
        let input = vec![b'x'; 4096];

        let (output, _dt) = compressor
            .compress(true, DataTypeFlag::None, &input)
            .unwrap();

        assert!(matches!(output, Cow::Owned(_)));
        assert!(output.len() < input.len());
    }
}
