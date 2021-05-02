import { pool, isMainThread, worker, workerEmit } from "workerpool";
import ioredis from "ioredis";
import glob from "glob";

import readline from "readline";
import prettyBytes from "pretty-bytes";
import { createReadStream, createWriteStream, statSync } from "fs";
import { ZSTDDecompress } from "simple-zstd";
import { InfluxDB, Point } from "@influxdata/influxdb-client";


const MESSAGE_MATCHES = [
    new RegExp(/\d+=(?<status_code>\d+)\s/),
    new RegExp(/(?<state>Starting|Finished) (?<action>\w+) for/),
    new RegExp(/(?<action>Tracker confirmed)/),
    new RegExp(/(?<action>Uploading with Rsync)/),
    new RegExp(/(?<action>Submitting) (?<items>\d+) items/),
];

let parse_message = (line) => {
    MESSAGE_MATCHES.forEach((regex) => {
        let match = line.match(regex);

        if (match) {
            try {
                let parsed = JSON.parse(line);
                workerEmit({
                    timestamp: parsed["@timestamp"],
                    host: parsed.host.name,
                    // message: parsed.message,
                    ...match.groups,
                });
            } catch(e) {
                return;
            }
        }
    });
};

let count_redis = async (redis) => {
    let last_message = "0";
    let i = 0;

    for (;;) {
        let messages = await redis.xread("COUNT", "2048", "STREAMS", "filebeat", last_message);

        // No messages, request again.
        if (!messages) break;

        let responses = messages[0][1];
        for (let message of responses) {
            last_message = message[0];
            parse_message(message[1][1]);
            i += 1
        }

        console.log(i);
    }
};

let parse_file = (filename) => {
    return new Promise((resolve, reject) => {
        try {
            const lineStream = readline.createInterface({
                input: createReadStream(filename)
                    .pipe(ZSTDDecompress()),
                crlfDelay: Infinity
            });

            let i = 0;
            let bytes = 0;
            let last_check = new Date().getTime();
            
            lineStream
                .on("line", (line) => {
                    i += 1;
                    bytes += line.length;
                    
                    if (i % 1048576 == 0) {
                        let new_time = new Date().getTime();
                        let delta = new_time - last_check;
                        last_check = new_time;
                        console.log(filename, i.toLocaleString(), prettyBytes(bytes), delta / 1000.0);
                    }
                    
                    parse_message(line);
                })
                .on("close", () => {
                    let new_time = new Date().getTime();
                    let delta = new_time - last_check;
                    last_check = new_time;
                    console.log(filename, i.toLocaleString(), prettyBytes(bytes), "completed", delta / 1000.0);
                    resolve({
                        lines: i,
                        bytes: bytes,
                    });
                });
        } catch(e) {
            reject(e);
        }

    });
};

// https://github.com/chetandhembre/node-influxdb-line-protocol/blob/master/index.js
function influx_escape(str) {
    return str.split('').map(function (character) {
      if (character === ' ' || character === ',') {
        character = '\\' + character;
      }
      return character;
    }).join('');
}

let on_worker_event = (payload, writeApi) => {
    // Alter data
    if (payload.state) payload.state = payload.state.toLowerCase();

    // Convert timestamp to microseconds.
    let timestamp = Date.parse(payload.timestamp);
    delete payload["timestamp"];

    // do something here
    let point =
        new Point("log")
        .tag("type", "warrior")
        .timestamp(timestamp)
        .fields;

    writeApi.writePoint(point);
};

let main = async (redis) => {
    // Create Influx.
    const influxDB = new InfluxDB({url, token});
    const writeApi = influxDB.getWriteApi(org, bucket)

    // Create work pool
    const work_pool = pool("./icqa.mjs", {
        maxWorkers: 8,
    });

    glob("output/*.txt.zst", async (error, files) => {
        // Sort the files by mtime.
        // https://stackoverflow.com/questions/10559685/using-node-js-how-do-you-get-a-list-of-files-in-chronological-order
        files.sort(function(a, b) {
            return statSync(b).mtime.getTime() - 
                   statSync(a).mtime.getTime();
        });

        // Create promises for all files.
        let file_promises = [];
        for (let file of files) {
            file_promises.push(work_pool.exec("parse_file", [file], {
                on: (payload) => on_worker_event(payload, writeApi),
            }));
        }

        // Await for all promises to complete.
        let file_lines = await Promise.all(file_promises);
        let lines_sum = 0;
        let bytes_sum = 0;
        for (let result of file_lines) {
            console.log(result);
            lines_sum += result.lines;
            bytes_sum += result.bytes;
        }
        console.log(lines_sum.toLocaleString(), "total lines");
        console.log(prettyBytes(bytes_sum), "total bytes");
    });

    writeApi
        .close()
        .then(() => {
            console.log('FINISHED ... now try ./query.ts')
        });
};

// Run main and cleanup redis if we are the main thread.
if (isMainThread) {
    const local_client = new ioredis();

    try {
        main();
    } finally {
        local_client.disconnect();
    }
} else {
    console.log("is worker");
    worker({
        parse_file: parse_file
    });
}