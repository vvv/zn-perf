use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

fn bench(c: &mut Criterion) {
    let parquet_file = zn_perf::new_mem_reader(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013506939548213248.parquet"
    ))
    .unwrap();

    let mut group = c.benchmark_group("parquet-io");
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("read-all", |b| {
        b.iter(|| zn_perf::read_all_data(&parquet_file).unwrap())
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
