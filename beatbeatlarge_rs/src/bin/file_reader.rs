use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};
use std::time::Instant;
use glob::glob;
use mimalloc::MiMalloc;
use chrono::{DateTime, Utc};

use simdjson_rust::dom;
use rayon::prelude::*;
use influxdb::Query;
use influxdb::InfluxDbWriteable;
use std::convert::TryInto;
use regex::Regex;
use lazy_static::lazy_static;
use maplit::hashmap;

use beatbeatlarge::structs::{BeatContainerMetadata, Message};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn json_get_str(data: &simdjson_rust::dom::object::Object, key: &str) -> Option<String> {
    match data.at_pointer(key) {
        Ok(value) => Some(value.get_string().unwrap()),
        Err(_) => None
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

fn grok_message(data: Message, grok_fields: &HashMap<&str, &str>) -> Option<String> {
    lazy_static! {
        static ref GROK_REGEXES: Vec<Regex> = vec![
            Regex::new(r"^(?P<pipeline_stage>Starting|Failed|Finished) (?P<pipeline_task>\w+) for Item").unwrap(),
            Regex::new(r"^\d+=(?P<status_code>\d+) ").unwrap(),
            Regex::new(r"sent (?P<rsync_sent>[\d,]+) bytes\s\sreceived (?P<rsync_received>[\d,]+) bytes\s\s(?P<rsync_throughput>[\d,\.]+) bytes").unwrap(),
            Regex::new(r"^(?P<pipeline_stage>Initializing) pipeline for '(?P<pipeline_name>.+)'").unwrap(),
        ];
    }
    let mut query = influxdb::Timestamp::Milliseconds(data.timestamp.timestamp_millis().try_into().unwrap())
        .into_query("metric")
        .add_tag("host", data.hostname);

    // grok magics
    let mut got_fields = false;

    for grok_r in GROK_REGEXES.iter() {
        match grok_r.captures(&data.message) {
            Some(caps) => {
                for _cap_name in grok_r.capture_names() {
                    match _cap_name {
                        Some(cap_name) => {
                            query = query.add_field(
                                cap_name,
                                caps.name(cap_name).unwrap().as_str()
                            );
                            got_fields = true;
                        },
                        None => {}
                    }
                };

                break;
            },
            None => {}
        }
    };

    // static line bs
    for (action_text, action) in grok_fields.into_iter() {
        if data.message.starts_with(action_text) {
            query = query.add_field("warrior_action", action);
            got_fields = true;
            break;
        }
    }

    // Add container name and image if given.
    match data.container {
        Some(container) => {
            // Add image
            match container.image {
                Some(container_image) => {
                    // if !got_fields && container_image.starts_with("atdr") {
                    //     println!("{}", data.message);
                    // }

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

    if got_fields {
        return Some(query.build().unwrap().get());
    } else {
        return None;
    }
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

    // XXX / TODO: hacky grok
    let warrior_actions = hashmap!{
        "Received item" => "item_received",
        "Queued file" => "queued_file",
        "Queued user" => "queued_user",
        "Queuing URL" => "queued_url",
        "Queuing folder" => "queued_folder",
        "Checking IP address" => "ip_check",
        "Tracker confirmed item" => "item_confirmed",
        "Uploading with Rsync" => "item_uploading",
        "No item received." => "no_items"
    };

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
        match grok_message(message, &warrior_actions) {
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
