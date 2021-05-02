import process from "process";
import {createWriteStream} from "fs";
import dateformat from "dateformat";
import stream from "stream";

import ioredis from "ioredis";

// General
const MAX_FILE_SIZE = 1024 * 1024 * 1024; // 1GB log files are enough.
var running = true;

// Setup Redis.
const remote_client = new ioredis({
    host: process.env.REDIS_HOST,
    password: process.env.REDIS_PASSWORD,
});
const local_client = new ioredis();

// Create stream.
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
let last_item = "$";

let main = async () => {
    while (running) {
        let read_response;

        // Attempt to fetch items.
        try {
            read_response = await local_client.xread("BLOCK", "50", "COUNT", "50", "STREAMS", "filebeat", last_item); 
        } catch(e) {
            await delay(500);
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
            try {
                await process_item(JSON.parse(payload));
            } catch(e) {
                console.log(payload);
                console.error(e);
            }
        }
    }
};

let process_item = async (data) => {
    let send = false;

    if (data.container) {
        if (data.container.image.name === "atdr.meo.ws/archiveteam/yahooanswers-grab") {
            send = true;
        }
    }

    if (send) {
        let container_name = (data.container || {}).name;
        let result = {
            host: data.host.name,
            message: data.message,
        }

        if (container_name) result.container = container_name;

        await remote_client.publish("beatbeatlarge", JSON.stringify(result));
        console.log(result.message)
    }
}

await main()