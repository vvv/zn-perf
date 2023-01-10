## How to build

Compile with one of the following commands:

```sh
# Enable all features supported by the current CPU:
RUSTFLAGS='-C target-cpu=native' cargo build --release
```

or

```sh
# Enable all features supported by the current CPU, and enable full use of AVX512:
RUSTFLAGS='-C target-cpu=native -C target-feature=-prefer-256-bit' \
    cargo build --release
```

To see the features enabled by `RUSTFLAGS`, type

```bash
diff -u <(rustc --print cfg) <(rustc -C target-cpu=native --print cfg)
```

See also https://crates.io/crates/arrow#performance-tips

## How to obtain parquet files

1. Ask someone nicely :wink:
2. Generate a parquet file with [`tustvold/access-log-gen`](https://github.com/tustvold/access-log-gen)
