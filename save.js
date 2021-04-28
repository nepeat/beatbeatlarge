const process = require("process");
const ioredis = require("ioredis");

const sub_client = new ioredis({
    host: process.env.REDIS_HOST,
    password: process.env.REDIS_PASSWORD,
});

const local_client = new ioredis();
const MAX_MESSAGES = "2000000";

sub_client.subscribe("filebeat");

sub_client.on("message", (channel, payload) => {
    local_client.xadd(channel, "MAXLEN", "~", MAX_MESSAGES, "*", "message", payload)
});