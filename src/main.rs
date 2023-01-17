use clap::Parser;
use itertools::Itertools;
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    basic::Type as PhysicalType,
    file::{footer, reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::{fs::File, path::PathBuf};
use zn_perf::ZnResult;

#[derive(Debug, Parser)]
#[command(about)]
struct Args {
    /// Path to a parquet file
    file: PathBuf,
}

fn main() -> ZnResult<()> {
    let args = Args::parse();
    let path = args.file;

    // `parquet::file` API
    let file = File::open(&path)?;
    let file = SerializedFileReader::new(file)?;
    dbg!(zn_perf::file::count_occurrences(&file, b"us-west-2")?);
    let file_metadata = file.metadata().file_metadata();
    println!(
        "{} has {} rows in {} row group(s)",
        path.display(),
        file_metadata.num_rows(),
        file.num_row_groups()
    );

    // `parquet::arrow` API
    let file = File::open(&path)?;
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;
    dbg!(zn_perf::arrow::count_occurrences(
        parquet_reader,
        "us-west-2"
    )?);

    // Query metadata
    let file = File::open(path)?;
    let metadata = footer::parse_metadata(&file)?;
    let schema = metadata.file_metadata().schema_descr();

    for row_group in metadata.row_groups() {
        for (column, column_schema) in row_group.columns().iter().zip_eq(schema.columns()) {
            assert!(column.column_descr().eq(column_schema));
            if !matches!(
                column_schema.physical_type(),
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ) {
                println!(
                    "XXX {:?} {:?}",
                    column_schema.physical_type(),
                    column_schema.name()
                );
            }
        }
    }

    Ok(())
}
