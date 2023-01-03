use arrow_array::cast;
use arrow_schema::DataType;
use clap::Parser;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::FileReader};
use std::{fs::File, path::PathBuf};
use zn_perf::ZnResult;

#[derive(Debug, Parser)]
#[command(about)]
struct Cli {
    /// Input files
    files: Vec<PathBuf>,
}

fn main() -> ZnResult<()> {
    let cli = Cli::parse();
    for path in cli.files {
        // `parquet::file` API
        let file = zn_perf::new_file_reader(&path)?;
        dbg!(zn_perf::count_occurrences(&file, b"us-west-2")?);
        let file_metadata = file.metadata().file_metadata();
        println!(
            "{} has {} rows in {} row group(s)",
            path.display(),
            file_metadata.num_rows(),
            file.num_row_groups()
        );

        // `parquet::arrow` API
        let file = File::open(path)?;
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;
        let mut nr_occurrences = 0;
        for batch in parquet_reader {
            let batch = batch?;
            for array in batch.columns() {
                match array.data_type() {
                    DataType::Utf8 => {
                        let array = cast::as_string_array(array);
                        nr_occurrences += array
                            .iter()
                            .flatten()
                            .filter(|s| s.contains("us-west-2"))
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
        dbg!(nr_occurrences);
    }
    Ok(())
}
