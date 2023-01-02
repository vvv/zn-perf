use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet::file::reader::FileReader;
use std::time::Duration;

fn bench(c: &mut Criterion) {
    let parquet_reader = zn_perf::new_mem_reader(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013508450449760256.parquet"
    ))
    .unwrap();

    let mut total_byte_size: i64 = 0;
    for row_group in parquet_reader.metadata().row_groups() {
        total_byte_size += row_group.total_byte_size();
    }
    let total_byte_size: u64 = total_byte_size.try_into().unwrap();

    let mut group = c.benchmark_group("parquet");
    group
        .measurement_time(Duration::from_secs(30))
        .throughput(Throughput::Bytes(total_byte_size));

    group.bench_function("read-everything", |b| {
        b.iter(|| zn_perf::read_all_data(&parquet_reader).unwrap())
    });
    group.bench_function("count-occurrences-in-str-columns", |b| {
        b.iter(|| zn_perf::count_occurrences(&parquet_reader, b"search_string").unwrap())
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
