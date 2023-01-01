use clap::Parser;
// use parquet::file::reader::{FileReader, SerializedFileReader};
// use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(about)]
struct Cli {
    /// Input files
    files: Vec<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    /* XXX
    let file = File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/dat/7013506939548213248.parquet"
    ))?;
    let file = SerializedFileReader::new(file)?;
    assert_eq!(file.num_row_groups(), 1);

    let row_group = file.get_row_group(0)?;
    assert_eq!(row_group.num_columns(), 41);
    // dbg!(row_group.metadata());
    // XXX */
    Ok(())
}
