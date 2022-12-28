use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let f = File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013506939548213248.parquet"
    ))?;
    let rd = SerializedFileReader::new(f)?;
    assert_eq!(rd.num_row_groups(), 1);
    Ok(())
}
