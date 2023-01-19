use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream::StreamExt;
use itertools::Itertools;
use parquet::{
    arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::{env, fs, time::Duration};
use tokio::runtime::Runtime;

fn parquet_sample_path() -> String {
    env::var("FILE").expect("Set FILE environment variable")
}

fn new_parquet_file_reader() -> SerializedFileReader<Bytes> {
    let buf = fs::read(parquet_sample_path()).unwrap(); // load the entire file into memory
    SerializedFileReader::new(buf.into()).unwrap()
}

fn new_parquet_arrow_reader() -> ParquetRecordBatchReader {
    let buf = fs::read(parquet_sample_path()).unwrap(); // load the entire file into memory
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
    //
    // See also:
    // - https://arrow.apache.org/datafusion/user-guide/configs.html
    // - https://github.com/apache/arrow-datafusion/blob/9bee14ebd39dacbb66a9b1f34cd6494bc6a6be3f/datafusion/core/src/config.rs#L61
    let config = SessionConfig::default()
        .with_collect_statistics(true)
        .with_batch_size(8 * 1024)
        // use parquet data page level metadata (Page Index) statistics to
        // reduce the number of rows decoded
        .set_bool("datafusion.execution.parquet.enable_page_index", true)
        // filter expressions are be applied during the parquet decoding
        // operation to reduce the number of rows decoded
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        // filter expressions evaluated during the parquet decoding opearation
        // will be reordered heuristically to minimize the cost of evaluation
        .set_bool("datafusion.execution.parquet.reorder_filters", true);

    let ctx = SessionContext::with_config(config);
    ctx.register_parquet("tbl", &parquet_sample_path(), Default::default())
        .await
        .unwrap();
    ctx
}

fn bench_datafusion_queries(c: &mut Criterion) {
    return;
    const QUERIES: &[&str] = &[
        "select * from tbl",
        r#"select * from tbl where "kubernetes.labels.operator.prometheus.io/name" = 'k8s'"#,
        r#"select * from tbl where kubernetes.labels.controller-revision-hash like '%ziox%'"#,
        "select * from tbl where log like '%k8s%'",
        "select * from tbl where strpos(log, 'k8s') > 0",
    ];

    let mut group = c.benchmark_group("DF");
    group.measurement_time(Duration::from_secs(15));

    let rt = Runtime::new().unwrap();
    for query in QUERIES {
        group.bench_with_input(BenchmarkId::new("queries", query), query, |b, i| {
            b.to_async(&rt).iter(|| async {
                let ctx = new_datafusion_session_context().await;
                let df = ctx.sql(i).await.unwrap();
                let mut stream = df.execute_stream().await.unwrap();
                while let Some(batch) = stream.next().await {
                    let _ = batch.unwrap().num_rows();
                    unreachable!("XXX");
                }
            })
        });
    }
}

fn bench_datafusion_search(c: &mut Criterion) {
    let f = fs::File::open(parquet_sample_path()).unwrap();

    let mut total_size = 0; // uncompressed size of text columns
    let where_clause = zn_perf::metadata::text_columns(&f)
        .unwrap()
        .iter()
        .map(|(name, size)| {
            total_size += *size;
            format!("\"{name}\" like '%k8s%'")
        })
        .join(" or ");
    let sql = format!("select * from tbl where {where_clause}");
    dbg!(("XXX", &sql));

    let mut group = c.benchmark_group("DF");
    group.throughput(Throughput::Bytes(total_size));

    let rt = Runtime::new().unwrap();
    group.bench_with_input(
        BenchmarkId::new("search", &sql),
        &sql,
        |b, i| {
            b.to_async(&rt).iter(|| async {
                let ctx = new_datafusion_session_context().await;
                let df = ctx.sql(i).await.unwrap();
                let mut stream = df.execute_stream().await.unwrap();
                while let Some(batch) = stream.next().await {
                    let _ = batch.unwrap().num_rows();
                    unreachable!("XXX");
                }
            })
        },
    );
}

criterion_group!(
    benches,
    bench_file_search,
    bench_arrow_search,
    bench_datafusion_queries,
    bench_datafusion_search,
);
criterion_main!(benches);
