"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
//const app = require("express")()
//app.listen(9000, () => console.log("Listening to port 9000"))
const clientId = "myProducerId";
// Setting up Kafka
const brokers = ["127.0.0.1:9092"];
const kafka = new kafkajs_1.Kafka({
    logLevel: kafkajs_1.logLevel.ERROR,
    brokers: brokers,
    clientId: clientId,
});
console.log("Meine ClientID: " + clientId);
const times = Number(process.argv[2]);
const topic = process.argv[3];
function connectToKafka() {
    return __awaiter(this, void 0, void 0, function* () {
        const producer = kafka.producer({ createPartitioner: kafkajs_1.Partitioners.LegacyPartitioner });
        yield producer.connect();
        return producer;
    });
}
function produceMessages() {
    return __awaiter(this, void 0, void 0, function* () {
        const producer = yield connectToKafka();
        for (let i = 0; i < times; i++) {
            const msg = generateMessage();
            console.log("Generated Message");
            console.log(msg);
            yield producer.send({ topic: topic, messages: Array.of(msg) });
        }
    });
}
function generateMessage() {
    const number = Math.floor(Math.random() * 100000);
    const message = {
        key: "myKey",
        value: JSON.stringify({
            product: {
                id: number
            }
        })
    };
    return message;
}
produceMessages();
