mod error;

use bytes::Bytes;
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
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

pub fn read_all_data<R: FileReader>(file_reader: &R) -> ZnResult<()> {
    let mut row_iter = file_reader.get_row_iter(None)?;
    assert!(row_iter.all(|row| {
        // Consume the row by converting it into JSON value. Then consume the
        // JSON value by comparing it with `null`.
        !row.to_json_value().is_null()
    }));
    Ok(())
}

/* XXX
pub fn parquet_file_contains<P: AsRef<Path>>(path: P, needle: &[u8]) -> ZnResult<bool> {
    assert!(!needle.is_empty());

    todo!()
}
// XXX */
