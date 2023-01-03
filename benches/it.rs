use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use parquet::{
    arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::{fs, time::Duration};

const INPUT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/dat/7013508450449760256.parquet"
);

fn new_parquet_file_reader() -> SerializedFileReader<Bytes> {
    let buf = fs::read(INPUT).unwrap(); // load the entire file into memory
    SerializedFileReader::new(buf.into()).unwrap()
}

fn new_parquet_arrow_reader() -> ParquetRecordBatchReader {
    let file = fs::File::open(INPUT).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap()
}

fn bench_file_search(c: &mut Criterion) {
    let parquet_reader = new_parquet_file_reader();

    let size = zn_perf::file::byte_array_columns_uncompressed_size(parquet_reader.metadata());

    let mut group = c.benchmark_group("file-search");
    group
        .measurement_time(Duration::from_secs(15))
        .throughput(Throughput::Bytes(size));

    group.bench_function("count-occurrences", |b| {
        b.iter(|| zn_perf::file::count_occurrences(&parquet_reader, b"search_string").unwrap())
    });
    group.finish();
}

fn bench_arrow_search(c: &mut Criterion) {
    let size: usize = new_parquet_arrow_reader()
        .into_iter()
        .map(|batch| batch.unwrap().get_array_memory_size())
        .sum();

    let mut group = c.benchmark_group("arrow-search");
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("count-occurrences", |b| {
        b.iter_batched(
            new_parquet_arrow_reader,
            |parquet_reader| {
                zn_perf::arrow::count_occurrences(parquet_reader, "search_string").unwrap()
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, bench_file_search, bench_arrow_search);
criterion_main!(benches);
