use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
use std::time::Duration;

fn parquet_file_reader() -> SerializedFileReader<Bytes> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013508450449760256.parquet"
    );
    let buf = std::fs::read(path).unwrap(); // load the entire file into memory
    SerializedFileReader::new(buf.into()).unwrap()
}

fn bench_file_read(c: &mut Criterion) {
    let parquet_reader = parquet_file_reader();

    let mut total_byte_size: i64 = 0;
    for row_group in parquet_reader.metadata().row_groups() {
        total_byte_size += row_group.total_byte_size();
    }
    let total_byte_size: u64 = total_byte_size.try_into().unwrap();

    let mut group = c.benchmark_group("file-read");
    group
        .measurement_time(Duration::from_secs(30))
        .throughput(Throughput::Bytes(total_byte_size));

    group.bench_function("everything", |b| {
        b.iter(|| zn_perf::file::read_all_data(&parquet_reader).unwrap())
    });
    group.finish();
}

fn bench_file_search(c: &mut Criterion) {
    let parquet_reader = parquet_file_reader();

    let size = zn_perf::file::byte_array_columns_uncompressed_size(parquet_reader.metadata());

    let mut group = c.benchmark_group("file-search");
    group
        .measurement_time(Duration::from_secs(15))
        .throughput(Throughput::Bytes(size));

    group.bench_function("count-occurrences-in-str-columns", |b| {
        b.iter(|| zn_perf::file::count_occurrences(&parquet_reader, b"search_string").unwrap())
    });
    group.finish();
}

criterion_group!(benches, bench_file_read, bench_file_search);
criterion_main!(benches);
