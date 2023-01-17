use crate::ZnResult;
use parquet::{
    basic::Type as PhysicalType,
    file::{footer, reader::ChunkReader},
};
use std::collections::HashMap;

/// Returns names and uncompressed data sizes (in bytes) of columns that are of
/// [`PhysicalType::BYTE_ARRAY`] or [`PhysicalType::FIXED_LEN_BYTE_ARRAY`] type.
pub fn text_columns<R: ChunkReader>(chunk_reader: &R) -> ZnResult<Vec<(String, u64)>> {
    let metadata = footer::parse_metadata(chunk_reader)?;

    let mut col_sizes = HashMap::new();
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let column_descr = column.column_descr();
            if matches!(
                column_descr.physical_type(),
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
            ) {
                let size: i64 = column.uncompressed_size();
                assert!(size >= 0);
                let size = size as u64;
                col_sizes
                    .entry(column_descr.name().to_owned())
                    .and_modify(|n| *n += size)
                    .or_insert(size);
            }
        }
    }

    Ok(metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .filter_map(|column_schema| {
            let name = column_schema.name();
            col_sizes.get(name).map(|size| (name.to_owned(), *size))
        })
        .collect())
}
