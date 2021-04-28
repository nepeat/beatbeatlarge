const ioredis = require("ioredis");

const local_client = new ioredis();

let last_message = "$";

let main = async () => {
    while (true) {
        let messages = await local_client.xread("BLOCK", "100", "COUNT", "100", "STREAMS", "filebeat", last_message);

        // No messages, request again.
        if (!messages) continue;

        let responses = messages[0][1];
        for (let message of responses) {
            last_message = message[0];
            payload = message[1][1];
            on_message(payload);
        }
    }
}

let on_message = (payload) => {
    let message = JSON.parse(payload);

    let container_name = (message.container || {}).name;
    let print_name = message.host.name;

    if (container_name) print_name = print_name + ", " + container_name;

    console.log(`${print_name} - ${message.message}`);
};

main();