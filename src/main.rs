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
    Ok(())
}
