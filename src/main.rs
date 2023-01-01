use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::PathBuf};

#[derive(Debug, Parser)]
#[command(about)]
struct Cli {
    /// Input files
    files: Vec<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    for path in cli.files {
        let file = File::open(&path)?;
        let file = SerializedFileReader::new(file)?;
        let file_metadata = file.metadata().file_metadata();
        println!(
            "{} has {} rows in {} row group(s)",
            path.display(),
            file_metadata.num_rows(),
            file.num_row_groups()
        );
        for (i, col) in file_metadata.schema().get_fields().iter().enumerate() {
            println!("    column {i}: {}", col.name());
        }
    }
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
