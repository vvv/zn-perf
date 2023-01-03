//! Read Parquet files using the [`parquet::arrow`] API
//!
//! [`parquet::arrow`]: https://docs.rs/parquet/latest/parquet/arrow/index.html

use crate::ZnResult;
use arrow_array::cast;
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

/// XXX-DOCUMENTME
pub fn count_occurrences(haystack: ParquetRecordBatchReader, needle: &str) -> ZnResult<usize> {
    let mut count = 0;
    for batch in haystack {
        let batch = batch?;
        for array in batch.columns() {
            match array.data_type() {
                DataType::Utf8 => {
                    let array = cast::as_string_array(array);
                    count += array
                        .iter()
                        .flatten()
                        .filter(|s| s.contains(needle))
                        .count();
                }
                DataType::Int64 => (),
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::LargeBinary
                | DataType::LargeUtf8
                | DataType::List(_)
                | DataType::FixedSizeList(_, _)
                | DataType::LargeList(_)
                | DataType::Struct(_)
                | DataType::Union(_, _, _)
                | DataType::Dictionary(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Map(_, _) => todo!("{:#?}", array.data_type()),
            }
        }
    }
    Ok(count)
}
