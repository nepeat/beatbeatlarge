import process from "process";
import {createWriteStream} from "fs";
import dateformat from "dateformat";
import stream from "stream";

import prettyBytes from "pretty-bytes";
import { ZSTDCompress } from "simple-zstd";
import ioredis from "ioredis";

// General
const MAX_FILE_SIZE = 1024 * 1024 * 256; // 256MB log files.
var running = true;

// Setup Redis.
const local_client = new ioredis();

// Create stream.
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
let last_item = await local_client.get("filebeat_last") || "$";

let worker = async () => {
    // Stats state
    let bytes = 0;
    let processed = 0;

    // Streams for the file pipeline..
    let compressor_stream = ZSTDCompress(8);
    let output_file = createWriteStream("output/" + Math.floor(new Date().getTime() / 1000) + "-logs-" + dateformat(new Date(), "yyyy-mm-dd-HH-MM-ss") + ".txt.zst");
    let read_stream = new stream.Readable({
        read: async (size) => {
            let bytes_get = 0;
            let tries = 0;

            // Stop attempting to read more from Redis if the output file is larger than the max.
            if (output_file.bytesWritten > MAX_FILE_SIZE) return;

            // We are willing to query Redis 3 times before giving up the data to the compressor.
            while (bytes_get < size) {
                let read_response;

                // Reset the last item pointer to the last item if we have been trying for 10 seconds.
                if (tries > 10) {
                    last_item = "$";
                }

                // Attempt to fetch items.
                try {
                    read_response = await local_client.xread("BLOCK", "500", "COUNT", "250", "STREAMS", "filebeat", last_item); 
                } catch(e) {
                    await delay(1000);
                    console.error(e);
                    continue;
                }

                // Retry if we were unable to get more items.
                if (!read_response) {
                    continue;
                }

                let items = read_response[0][1];
                for (let item of items) {
                    last_item = item[0];
                    let payload = item[1][1];
                    // Add the item to the file.
                    read_stream.push(payload);
                    read_stream.push("\n");

                    // Update the stat keeping variables first.
                    bytes += payload.length;
                    bytes_get += payload.length;
                    processed += 1;
                }

                // Update the last item pointer stored in Redis.
                await local_client.set("filebeat_last", last_item);

                // Increment the tries counter by 1.
                tries += 1;
            }
        }
    });

    compressor_stream.on("end", () => {
        console.log("ended compressor");
        output_file.end();
    });

    // Exit the process if we are exitting and all writes are complete.
    output_file.on("finish", () => {
        if (!running) process.exit();
    })

    // Start the stream.
    console.log("starting pipe to file");
    read_stream.pipe(compressor_stream).pipe(output_file);
    while (running && output_file.bytesWritten < MAX_FILE_SIZE) {
        await delay(1000);
        console.log(`${prettyBytes(bytes)} bytes read, ${prettyBytes(output_file.bytesWritten)} bytes written, ${processed} processed`)
    }
    console.log("loop done, ending");
    compressor_stream.end();
};

let main = async () => {
    await worker();
    if (running) {
        setTimeout(main, 1000);
    }
};

process.on('SIGINT', function() {
    console.log("Caught interrupt signal");
    running = false;
});

main()