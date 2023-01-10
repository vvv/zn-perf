use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::{
    config::{
        OPT_PARQUET_ENABLE_PAGE_INDEX, OPT_PARQUET_PUSHDOWN_FILTERS, OPT_PARQUET_REORDER_FILTERS,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::stream::StreamExt;
use parquet::{
    arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::{fs, time::Duration};
use tokio::runtime::Runtime;

// XXX FIXME: Read the path from environment variable
const INPUT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/dat/7013508450449760256.parquet"
);

fn new_parquet_file_reader() -> SerializedFileReader<Bytes> {
    let buf = fs::read(INPUT).unwrap(); // load the entire file into memory
    SerializedFileReader::new(buf.into()).unwrap()
}

fn new_parquet_arrow_reader() -> ParquetRecordBatchReader {
    let buf = fs::read(INPUT).unwrap(); // load the entire file into memory
    ParquetRecordBatchReaderBuilder::try_new(<Vec<u8> as Into<Bytes>>::into(buf))
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

async fn new_datafusion_session_context() -> SessionContext {
    // These configuration settings originate from
    // https://github.com/tustvold/access-log-bench/blob/b4bdc3895bb16b9e6246332946d085264b8949cd/datafusion/src/main.rs#L27-L32
    let config = SessionConfig::default()
        .with_collect_statistics(true)
        .with_batch_size(8 * 1024)
        .set_bool(OPT_PARQUET_ENABLE_PAGE_INDEX, true)
        .set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, true)
        .set_bool(OPT_PARQUET_REORDER_FILTERS, true);

    let ctx = SessionContext::with_config(config);
    ctx.register_parquet(
        "logs",
        INPUT,
        // concat!(env!("CARGO_MANIFEST_DIR"), "/dat/logs.parquet"),
        Default::default(),
    )
    .await
    .unwrap();
    ctx
}

fn bench_datafusion(c: &mut Criterion) {
    const QUERIES: &[&str] = &[
        "select * from logs",
        "select * from logs where 'kubernetes.labels.operator.prometheus.io/name' = 'k8s'",
        "select * from logs where 'kubernetes.labels.controller-revision-hash' like '%ziox%'",
        "select * from logs where log like '%k8s%'",
        // XXX TODO: Add a query that performs search in all text columns, e.g.
        // "select * from logs where c1 like '%spam%' or c2 like '%spam%' or ..."
    ];
    let rt = Runtime::new().unwrap();
    for query in QUERIES {
        c.bench_with_input(BenchmarkId::new("datafusion", query), query, |b, i| {
            b.to_async(&rt).iter(|| async {
                let ctx = new_datafusion_session_context().await;
                let df = ctx.sql(i).await.unwrap();
                let mut stream = df.execute_stream().await.unwrap();
                while let Some(batch) = stream.next().await {
                    let _ = batch.unwrap().num_rows();
                }
            })
        });
    }
}

criterion_group!(
    benches,
    bench_file_search,
    bench_arrow_search,
    bench_datafusion
);
criterion_main!(benches);
