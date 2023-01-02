mod error;

use bytes::Bytes;
use parquet::file::serialized_reader::SerializedFileReader;
use std::{fs::File, io::Read, path::Path};

pub use error::{ZnError, ZnResult};

pub fn new_file_reader<P: AsRef<Path>>(path: P) -> ZnResult<SerializedFileReader<File>> {
    let file = File::open(path)?;
    Ok(SerializedFileReader::new(file)?)
}

pub fn new_mem_reader<P: AsRef<Path>>(path: P) -> ZnResult<SerializedFileReader<Bytes>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    let _sz = file.read_to_end(&mut buf)?;
    Ok(SerializedFileReader::new(buf.into())?)
}

/* XXX
pub fn parquet_file_contains<P: AsRef<Path>>(path: P, needle: &[u8]) -> ZnResult<bool> {
    assert!(!needle.is_empty());

    todo!()
}
// XXX */
