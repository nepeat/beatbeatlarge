use redis::{Commands, Value};
use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply};
use std::env;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn create_redis() -> redis::Connection {
    let redis_host = env::var("REDIS_HOST").expect("missing REDIS_HOST");
    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();

    let redis_conn_url = format!("redis://:{}@{}", redis_password, redis_host);

    // redis::Client::open("redis_conn_url")
    // XXX LOL TEST
    redis::Client::open("redis://localhost")
        .expect("Invalid connection URL")
        .get_connection()
        .expect("failed to connect to Redis")
}

// read stream test
const STREAMS: &[&str] = &["filebeat"];

fn logs_read_stream() {
    let mut redis = create_redis();

    let mut last_id = String::from("$");
    let mut running = true;
    let mut parsed = 0;

    while running {
        let stream_options = StreamReadOptions::default()
            .block(1000)
            .count(1000);

        let read_reply: StreamReadReply = redis
            .xread_options(STREAMS, &[&last_id], &stream_options)
            .expect("read");

        for StreamKey { key, ids } in read_reply.keys {
            // last_id = ids.last().copied().to_string();
            for StreamId { id, map } in ids {
                last_id = id.to_string();

                if map.is_empty() {
                    running = false;
                }

                for (n, s) in map {
                    if let Value::Data(bytes) = s {
                        parsed += 1;
                        println!("{}", parsed);
                        // println!("\t\t{}: {}", n, String::from_utf8(bytes).expect("utf8"))
                    } else {
                        panic!("Weird data")
                    }
                }
            }
        }
    }
}

fn main() {
    logs_read_stream();
}
