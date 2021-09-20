use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};
use std::time::Instant;
use glob::glob;
use mimalloc::MiMalloc;
use chrono::{DateTime, Utc};

use simdjson_rust::dom;
use rayon::prelude::*;
use influxdb::{Query, Timestamp};
use influxdb::InfluxDbWriteable;
use std::convert::TryInto;

use beatbeatlarge::structs::{BeatContainerMetadata, Message};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


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
        timestamp: DateTime::parse_from_rfc3339(&data.at_key("@timestamp")?.get_string()?)?.with_timezone(&Utc),
        hostname: data.at_pointer("/host/name")?.get_string()?,
        message: data.at_key("message")?.get_string()?,
        container: container_meta
    };

    Ok(result)
}

fn grok_message(data: Message) -> Option<String> {
    let mut query = influxdb::Timestamp::Milliseconds(data.timestamp.timestamp_millis().try_into().unwrap())
        .into_query("metric")
        .add_tag("host", data.hostname);
    
    // Add container name and image if given.
    match data.container {
        Some(container) => {
            // Add image
            match container.image {
                Some(container_image) => {
                    query = query.add_tag("container_image", container_image);
                },
                None => ()
            };

            // Add name
            match container.name {
                Some(container_name) => {
                    query = query.add_field("container_name", container_name);
                },
                None => ()
            };
        },
        None => ()
    };

    query = query.add_field("action", "fart");

    if data.message.contains("drive") {
        return Some(query.build().unwrap().get());
    }

    None
}

fn files_read_stream(file_path: &std::path::PathBuf) -> std::io::Result<()> {
    // Functionwide parser
    let mut parser = dom::Parser::default();

    // Stat tracking
    let start_time = Instant::now();
    let mut lines = 0;
    let mut bytes = 0;

    println!("starting {}", file_path.display());

    // Open the zstd file
    let f = File::open(file_path)?;
    let reader = BufReader::new(f);
    let zstd_stream = zstd::stream::read::Decoder::with_buffer(reader)?;
    let zstd_reader = BufReader::new(zstd_stream);

    // Open output file
    let f_out = File::create(
        format!("parsed/{}.influx.gz", file_path.file_name().unwrap().to_str().unwrap().strip_suffix(".txt.zst").unwrap())
    )?;
    let mut zstd_writer = flate2::write::GzEncoder::new(f_out, flate2::Compression::fast());

    for compressed_line in zstd_reader.lines() {
        // decompress line
        let decompressed_line = match compressed_line {
            Ok(result) => {
                // add to byte count
                bytes += result.len();

                // line is cute and valid
                result
            },
            Err(e) => {
                eprintln!("Error decompressing in {}: {}", file_path.display(), e);
                break;
            }
        };

        // parse line to struct
        let message = match parser.parse(&decompressed_line) {
            Ok(data) => {
                match parse_line(data) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("Error parsing line. {} {}", decompressed_line, e);
                        continue;
                    }
                }
            },
            Err(e) => {
                eprintln!("Error parsing JSON in {}: {}", file_path.display(), e);
                break;
            }
        };

        lines += 1;

        // handle line
        match grok_message(message) {
            Some(line) => {
                zstd_writer.write(line.as_bytes())?;
                zstd_writer.write(b"\n")?;
            }
            None => ()
        };
    }

    // Final stats
    println!("done {} - {} lines, {} bytes, {} secs",
        file_path.display(),
        lines,
        bytes,
        start_time.elapsed().as_secs_f32()
    );

    // Cleanup and return
    zstd_writer.finish().unwrap();
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
