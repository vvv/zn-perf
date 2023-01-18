## How to build

```sh
# Enable all features supported by the current CPU:
RUSTFLAGS='-C target-cpu=native' cargo build --release
```

To see the features enabled by this `RUSTFLAGS` value, type

```bash
diff -u <(rustc --print cfg) <(rustc -C target-cpu=native --print cfg)
```

See also https://crates.io/crates/arrow#performance-tips

## How to run benchmarks

Specify the path to parquet file via `FILE` environment variable, e.g.

``` sh
FILE=dat/7013506939548213248.parquet \
RUSTFLAGS='-C target-cpu=native' \
cargo bench
```

## How to obtain parquet files

1. Ask someone nicely :wink:
2. Generate a parquet file with [`tustvold/access-log-gen`](https://github.com/tustvold/access-log-gen)
