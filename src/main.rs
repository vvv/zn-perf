// use parquet::file::reader::{FileReader, SerializedFileReader};
// use std::fs::File;

fn orig_test() {
    let main_text = "Microsoft surpassed expectations on the top and bottom lines, but cloud revenue was lower than expected, and the company's quarterly guidance fell short of expectations as well.";
    let terms = ["yahoo", "oracle", "microsoft", "funny", "7645674"];
    // let loops = 28_000_000;
    let loops = 64_000_000;

    for term in terms {
        let speed = basic_search(main_text, term, loops);
        println!("Searched for {} at {} MB/s", term, speed)
    }
}

fn basic_search(main_text: &str, term: &str, loops: u32) -> usize {
    let total_size = main_text.len() * loops as usize / 1024 / 1024;

    let start = std::time::Instant::now();
    for _ in 0..loops {
        let _ = find(main_text, term);
    }

    total_size / start.elapsed().as_secs() as usize
}

#[inline(always)]
#[cfg(target_arch = "x86_64")]
fn find(haystack: &str, needle: &str) -> bool {
    memchr::memmem::find(haystack.as_bytes(), needle.as_bytes()).is_some()
}

#[inline(always)]
#[cfg(not(target_arch = "x86_64"))]
fn find(haystack: &str, needle: &str) -> bool {
    haystack.contains(needle)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    orig_test();
    Ok(())
}
