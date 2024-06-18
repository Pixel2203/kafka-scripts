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
const ip = process.argv[2];
const brokers = [ip];
const clientId = "myConsumerId";
const kafka = new kafkajs_1.Kafka({
    logLevel: kafkajs_1.logLevel.ERROR,
    brokers: brokers,
    clientId: clientId,
});
function connectToKafka() {
    return __awaiter(this, void 0, void 0, function* () {
        const consumer = kafka.consumer({ groupId: "myconsumerd" });
        yield consumer.connect();
        return consumer;
    });
}
function consumeMessages() {
    return __awaiter(this, void 0, void 0, function* () {
        const consumer = yield connectToKafka();
        yield consumer.subscribe({ topics: ["com.bechtle.mails.v2", "com.bechtle.customers.user.keycloak.v1"] });
        console.log("Subscribed to topics");
        yield consumer.run({
            eachMessage: ({ topic, partition, message, heartbeat, pause }) => __awaiter(this, void 0, void 0, function* () {
                var _a;
                console.log("Read Message of topic " + topic);
                console.log((_a = message.value) === null || _a === void 0 ? void 0 : _a.toString());
            }),
        });
    });
}
consumeMessages();
