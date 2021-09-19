use std::collections::HashMap;
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
    message: String,
    container: Option<BeatContainerMetadata>
}

struct BeatContainerMetadata {
    id: String,
    name: Option<String>,
    image: Option<String>
    // labels: HashMap<String, String>
}

fn json_get_str(data: &simdjson_rust::dom::object::Object, key: &str) -> Option<String> {
    match data.at_pointer(key) {
        Ok(value) => Some(value.get_string().unwrap()),
        Err(e) => None
    }
}

fn parse_line(data: simdjson_rust::dom::element::Element) -> Result<Message, Box<dyn std::error::Error>> {
    let container_meta: Option<BeatContainerMetadata> = match data.at_key("container") {
        Ok(container_data) => {
            let container_data = container_data.get_object()?;
            Some(BeatContainerMetadata {
                id: container_data.at_key("id")?.get_string()?,
                name: json_get_str(&container_data, "/name"),
                image: json_get_str(&container_data, "/image/name")
            })
        }
        Err(_) => None
    };

    let result = Message {
        timestamp: data.at_key("@timestamp")?.get_string()?,
        hostname: data.at_pointer("/host/name")?.get_string()?,
        message: data.at_key("message")?.get_string()?,
        container: container_meta
    };

    Ok(result)
}

fn files_read_stream(file_path: &std::path::PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    println!("starting {}", file_path.display());

    let mut parser = dom::Parser::default();
    let f = File::open(file_path)?;
    let reader = BufReader::new(f);

    let zstd_stream = zstd::stream::read::Decoder::with_buffer(reader)?;
    let zstd_reader = BufReader::new(zstd_stream);


    let mut lines = 0;
    let mut hostmap: HashMap<String, u64> = HashMap::new();

    for line in zstd_reader.lines() {
        let parsed = match line {
            Ok(line) => {
                match parser.parse(&line) {
                    Ok(data) => {
                        match parse_line(data) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Error parsing line. {} {}", line, e);
                                continue;
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Error parsing JSON in {}: {}", file_path.display(), e);
                        break;
                    }
                }
            },
            Err(e) => {
                eprintln!("Error decompressing in {}: {}", file_path.display(), e);
                break;
            }
        };

        lines += 1;
        *hostmap.entry(parsed.hostname).or_insert(0) += 1;
    }

    println!("done {} - {} lines, {} secs",
        file_path.display(),
        lines,
        start_time.elapsed().as_secs_f32()
    );

    // for (host, count) in hostmap {
    //     println!("{}: {}", host, count);
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
            files_read_stream(x).unwrap_or_default();
        }).count();
}
