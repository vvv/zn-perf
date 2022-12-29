// use parquet::file::reader::{FileReader, SerializedFileReader};
// use std::fs::File;

fn orig_test() {
    let main_text = "Microsoft surpassed expectations on the top and bottom lines, but cloud revenue was lower than expected, and the company's quarterly guidance fell short of expectations as well.";
    let terms = ["yahoo", "oracle", "microsoft", "funny", "7645674"];

    for million_loops in [48, 64] {
        println!("\n{}M loops:", million_loops);
        for term in terms {
            let speed = basic_search(main_text, term, million_loops * 1_000_000);
            println!("Searched for {:?} at {} MB/s", term, speed);
        }
    }
}

fn basic_search(main_text: &str, term: &str, loops: u32) -> usize {
    let total_size = main_text.len() * loops as usize / 1024 / 1024;

    let start = std::time::Instant::now();
    for _ in 0..loops {
        assert!(!find(main_text, term));
    }

    total_size / start.elapsed().as_secs() as usize
}

fn find(haystack: &str, needle: &str) -> bool {
    /*
    // 64M loops:
    // Searched for yahoo at 2700 MB/s
    // Searched for oracle at 2700 MB/s
    // Searched for microsoft at 2700 MB/s
    // Searched for funny at 2700 MB/s
    // Searched for 7645674 at 3601 MB/s
    haystack.contains(needle)
    */

    // 48M loops:
    // Searched for yahoo at 8102 MB/s
    // Searched for oracle at 8102 MB/s
    // Searched for microsoft at 8102 MB/s
    // Searched for funny at 8102 MB/s
    // Searched for 7645674 at 8102 MB/s
    //
    // 64M loops:
    // Searched for yahoo at 10803 MB/s
    // Searched for oracle at 10803 MB/s
    // Searched for microsoft at 5401 MB/s
    // Searched for funny at 10803 MB/s
    // Searched for 7645674 at 10803 MB/s
    // Searched for well at 10803 MB/s
    memchr::memmem::find(haystack.as_bytes(), needle.as_bytes()).is_some()
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
