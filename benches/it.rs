use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| zn_perf::fibonacci(black_box(20))));
}

criterion_group!(benches, bench);
criterion_main!(benches);
