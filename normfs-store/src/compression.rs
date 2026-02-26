// Compression abstraction layer supporting both C bindings and pure Rust implementations
// When both features are enabled, pure-rust takes precedence for FreeBSD compatibility

// Zstd compression
pub fn zstd_compress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    #[cfg(feature = "pure-rust")]
    {
        // Use Best compression level (roughly level 11)
        return Ok(ruzstd::encoding::compress_to_vec(
            data,
            ruzstd::encoding::CompressionLevel::Best,
        ));
    }

    #[cfg(all(feature = "c-libs", not(feature = "pure-rust")))]
    {
        use std::io::Write;
        let mut encoder = zstd::Encoder::new(Vec::new(), 10)?;
        encoder.window_log(27)?;
        encoder.long_distance_matching(true)?;
        encoder.write_all(data)?;
        encoder.finish()
    }

    #[cfg(not(any(feature = "c-libs", feature = "pure-rust")))]
    compile_error!("Either 'c-libs' or 'pure-rust' feature must be enabled");
}

pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    #[cfg(feature = "pure-rust")]
    {
        use std::io::Read;
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        return Ok(decompressed);
    }

    #[cfg(all(feature = "c-libs", not(feature = "pure-rust")))]
    {
        zstd::decode_all(data)
    }

    #[cfg(not(any(feature = "c-libs", feature = "pure-rust")))]
    compile_error!("Either 'c-libs' or 'pure-rust' feature must be enabled");
}

// XZ/LZMA decompression (only used for reading legacy files)
// Use lzma-rust2 (pure Rust) for all features to avoid C dependencies
pub fn xz_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    use std::io::{Cursor, Read};
    let cursor = Cursor::new(data);
    let mut decoder = lzma_rust2::XzReader::new(cursor, true); // true = verify checksum
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}
