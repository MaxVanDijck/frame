use polars::prelude::*;
use polars_io::parquet::ParquetReader;
use polars_io::csv::CsvWriter;
use std::fs::File;

fn main() -> std::io::Result<()> {
    let command = std::env::args().nth(1).expect("no pattern given");
    let path = std::env::args().nth(2).expect("no path given");
    println!("pattern: {:?}, path: {:?}", command, path);

    match command.as_str() {
        "csv" => convert_parquet_to_csv(&path),
        _ => Ok(()),
    }
}

fn convert_parquet_to_csv(path: &str) -> std::io::Result<()> {
    let mut f = File::open(path)?;
    let mut df = ParquetReader::new(&mut f).finish().unwrap();
    let mut file = std::fs::File::create("test.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
    Ok(())
}
