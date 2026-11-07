use crate::{Compression, Error, Result};

pub(crate) const RAW: i64 = 0;
pub(crate) const ZSTD: i64 = 1;

pub(crate) async fn encode(
    bytes: Vec<u8>,
    compression: &Compression,
    threshold: usize,
    only_if_smaller: bool,
) -> Result<(Vec<u8>, i64)> {
    match compression {
        Compression::Disabled => Ok((bytes, RAW)),
        Compression::Zstd { .. } if bytes.len() < threshold => Ok((bytes, RAW)),
        Compression::Zstd { level } => {
            let level = *level;
            let original = bytes;
            let (original, compressed) = tokio::task::spawn_blocking(move || {
                zstd::stream::encode_all(&original[..], level)
                    .map(|compressed| (original, compressed))
            })
            .await?
            .map_err(Error::Compression)?;
            if only_if_smaller && compressed.len() >= original.len() {
                Ok((original, RAW))
            } else {
                Ok((compressed, ZSTD))
            }
        }
    }
}

pub(crate) async fn decode(bytes: Vec<u8>, encoding: i64) -> Result<Vec<u8>> {
    match encoding {
        RAW => Ok(bytes),
        ZSTD => tokio::task::spawn_blocking(move || zstd::stream::decode_all(&bytes[..]))
            .await?
            .map_err(Error::Compression),
        // Unknown encodings cannot safely be deserialized as raw bytes.
        _ => Err(Error::Compression(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported compression encoding {encoding}"),
        ))),
    }
}
