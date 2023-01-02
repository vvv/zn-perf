use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use once_cell::sync::Lazy;
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
use std::time::Duration;

static PARQUET_READER: Lazy<SerializedFileReader<Bytes>> = Lazy::new(|| {
    zn_perf::new_mem_reader(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013508450449760256.parquet"
    ))
    .unwrap()
});

fn bench_read(c: &mut Criterion) {
    let mut total_byte_size: i64 = 0;
    for row_group in PARQUET_READER.metadata().row_groups() {
        total_byte_size += row_group.total_byte_size();
    }
    let total_byte_size: u64 = total_byte_size.try_into().unwrap();

    let mut group = c.benchmark_group("read");
    group
        .measurement_time(Duration::from_secs(30))
        .throughput(Throughput::Bytes(total_byte_size));

    group.bench_function("everything", |b| {
        b.iter(|| zn_perf::read_all_data(&(*PARQUET_READER)).unwrap())
    });
    group.finish();
}

fn bench_search(c: &mut Criterion) {
    let size = zn_perf::byte_array_columns_uncompressed_size(PARQUET_READER.metadata());

    let mut group = c.benchmark_group("search");
    group
        .measurement_time(Duration::from_secs(15))
        .throughput(Throughput::Bytes(size));

    group.bench_function("count-occurrences-in-str-columns", |b| {
        b.iter(|| zn_perf::count_occurrences(&(*PARQUET_READER), b"search_string").unwrap())
    });
    group.finish();
}

criterion_group!(benches, bench_read, bench_search);
criterion_main!(benches);
