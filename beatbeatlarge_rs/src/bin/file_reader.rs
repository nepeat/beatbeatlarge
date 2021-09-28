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

enum CastType {
    Integer,
    Number,
}

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

        static ref GROK_TYPES: HashMap<&'static str, CastType> = hashmap! {
            "status_code" => CastType::Integer,
            "rsync_sent" => CastType::Number,
            "rsync_received" => CastType::Number,
            "rsync_throughput" => CastType::Number,
        };
    }
    let mut query = influxdb::Timestamp::Milliseconds(data.timestamp.timestamp_millis().try_into().unwrap())
        .into_query("metric");

    // Find the first grok regex than matches agains the message
    let first_captures = GROK_REGEXES.iter().find_map(|grok_r| {
        grok_r
            .captures(&data.message)
            // Ignore unnamed groups
            .zip(Some(grok_r.capture_names().flatten()))
    });
    let mut got_fields = first_captures.is_some();

    if let Some((caps, cap_names)) = first_captures {
        for cap_name in cap_names {
            let cap_value = &caps[cap_name];

            query =
                match GROK_TYPES.get(cap_name) {
                    Some(CastType::Integer) => query
                        .add_field(cap_name, cap_value.replace(",", "").parse::<i64>().unwrap()),

                    Some(CastType::Number) => query
                        .add_field(cap_name, cap_value.replace(",", "").parse::<f64>().unwrap()),

                    None => query.add_field(cap_name, cap_value),
                };
        }
    }

    // static line bs
    for (action_text, action) in grok_fields.into_iter() {
        if data.message.starts_with(action_text) {
            query = query.add_field("warrior_action", action);
            got_fields = true;
            break;
        }
    }

    // Stop if we do not have fields.
    if !got_fields {
        return None;
    }

    // Add the hostname.
    query = query.add_tag("host", data.hostname);

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

    Some(query.build().unwrap().get())
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
    let mut writer = flate2::write::GzEncoder::new(f_out, flate2::Compression::fast());

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
                writer.write_all(line.as_bytes()).unwrap();
                writer.write_all(b"\n").unwrap();
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
    writer.finish().unwrap();
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
