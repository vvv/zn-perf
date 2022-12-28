use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013506939548213248.parquet"
    ))?;
    let file = SerializedFileReader::new(file)?;
    assert_eq!(file.num_row_groups(), 1);

    let row_group = file.get_row_group(0)?;
    assert_eq!(row_group.num_columns(), 41);
    // dbg!(row_group.metadata());
    Ok(())
}
