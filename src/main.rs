use polars::prelude::*;
use polars_io::parquet::ParquetReader;
use polars_io::csv::CsvWriter;
use std::ffi::OsString;
use std::fs::File;
use std::path::PathBuf;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "frm")]
#[command(about = "A columnar filetype conversion tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Convert to csv filetype from parquet
    Csv {
        /// Path to file to convert
        #[arg(value_name = "PATH")]
        path: std::path::PathBuf,
        #[arg(value_name = "OUT")]
        /// Output path to save file to
        out: Option<OsString>,
    },
    /// Convert to parquet filetype from csv
    Parquet {
        /// Path to file to convert
        #[arg(value_name = "PATH")]
        path: std::path::PathBuf,
        #[arg(value_name = "OUT")]
        /// Output path to save file to
        out: Option<OsString>,
    },
}

fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Csv { path, out } => convert_to_csv(path, out),
        Commands::Parquet { path, out } => convert_to_parquet(path, out),
    }
}

fn convert_to_csv(path: PathBuf, out: Option<OsString>) -> std::io::Result<()> {
    let mut f = File::open(&path)?;
    let mut df = ParquetReader::new(&mut f).finish().unwrap();
    
    let out_path: OsString = match out {
        Some(x) => x,
        None => {
            let mut path_clone = path.clone(); // Clone the path to avoid borrowing issues
            path_clone.set_extension("csv"); // Set the extension to .csv
            path_clone.into_os_string() // Convert PathBuf to OsString
        },
    };
    let mut file = std::fs::File::create(out_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
    Ok(())
}

fn convert_to_parquet(path: PathBuf, out: Option<OsString>) -> std::io::Result<()> {
    let mut f = File::open(&path)?;
    let mut df = CsvReader::new(&mut f).finish().unwrap();
    
    let out_path: OsString = match out {
        Some(x) => x,
        None => {
            let mut path_clone = path.clone(); // Clone the path to avoid borrowing issues
            path_clone.set_extension("parquet"); // Set the extension to .csv
            path_clone.into_os_string() // Convert PathBuf to OsString
        },
    };
    let mut file = std::fs::File::create(out_path).unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();
    Ok(())
}
