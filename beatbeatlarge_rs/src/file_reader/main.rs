use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use glob::glob;
use mimalloc::MiMalloc;

use simdjson_rust::dom;
use rayon::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct Message {
    timestamp: String,
    hostname: String,
    message: String
}

fn parse_line(data: simdjson_rust::dom::element::Element) -> Result<Message, Box<dyn std::error::Error>> {
    let result = Message {
        timestamp: data.at_key("@timestamp")?.get_string()?,
        hostname: data.at_pointer("/host/name")?.get_string()?,
        message: data.at_key("message")?.get_string()?
    };

    Ok(result)
}

fn files_read_stream(file_path: &std::path::PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    println!("starting {}", file_path.display());

    let mut parser = dom::Parser::default();
    let f = File::open(file_path)?;
    let reader = BufReader::new(f);

    let zstd_stream = zstd::stream::read::Decoder::with_buffer(reader).unwrap();
    let zstd_reader = BufReader::new(zstd_stream);

    println!(
        "done {} - {} lines, {} secs",
        file_path.display(),
        zstd_reader
        .lines()
        .map(|line| {
            let line = line.unwrap_or_default();
            if !line.is_empty() {
                let data = parser.parse(&line).unwrap();
                parse_line(data).unwrap();
            }
        })
        .count(),
        start_time.elapsed().as_secs_f32()
    );

    // for line in zstd_reader.lines() {
    //     let line = line.unwrap();
    //     let v = parser.parse(&line).unwrap();
    //     let timestamp = v.at_key("@timestamp")?.get_string()?;
    //     let message = v.at_key("message")?.get_string()?;
    // }

    Ok(())
}

fn main() {
    let mut filenames = Vec::new();

    for entry in glob("../output/*.txt.zst").expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => filenames.push(path),
            Err(e) => println!("{:?}", e),
        }
    }

    filenames.par_iter()
        .map(|x| {
            files_read_stream(x);
        }).count();
}
