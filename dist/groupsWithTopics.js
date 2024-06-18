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
var consumer;
function connectToKafka(groupid) {
    return __awaiter(this, void 0, void 0, function* () {
        consumer = kafka.consumer({ groupId: groupid });
        yield consumer.connect();
    });
}
function disconnectFromKafka() {
    return __awaiter(this, void 0, void 0, function* () {
        yield consumer.disconnect();
    });
}
// Suchen die Offsets für alle Topics für die Gruppen
// Schauen welche Topics aktive Consumer haben in allen gruppen
// Das in eine Liste bündeln
// Doppelte einträge entfernen
// Liste mit den original topics abgleichen
// Ergebnis: Nicht konsumierte Topics
function fetchKafkaOffset(group) {
    return __awaiter(this, void 0, void 0, function* () {
        const result = yield kafka.admin().fetchOffsets({ groupId: group });
        console.log("Ergebnis für Gruppe : " + group);
        console.log(result);
        const desc = yield kafka.admin().describeGroups(Array.of(group));
    });
}
function readTopics() {
    return __awaiter(this, void 0, void 0, function* () {
        const result = yield consumer.describeGroup();
        console.log("Result:");
        // @ts-ignore
        console.log(result[0].topic);
        //await consumer.subscribe({topics: ["com.bechtle.mails.v2", "com.bechtle.customers.user.keycloak.v1"]})
        /*
        console.log("Subscribed to topics")
        await consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                console.log("Read Message of topic " + topic)
                console.log(message.value?.toString())
            },
        })
    
         */
    });
}
const groups = [
    "com.bechtle.price.v3",
    "com.bechtle.indexer",
    "price-handling-tool",
    "com.bechtle.find.XX",
    "kube-dev-author",
    "pht-catalog-sync-manager",
    "leadmanagement-backend",
    "cape-adapter-service",
    "com.bechtle.price.v3.tk4_CO2-1",
    "com.bechtle.best",
    "com.bechtle.beer-delivery",
    "com.bechtle.beus-api",
    "com.bechtle.kafka.mirror.dev.to.stage",
    "cape-adapter-service-retry-0",
    "com.bechtle.kafka.mirror.dev.to.test",
    "com.bechtle.hybris.dev",
    "com.bechtle.benjes",
    "com.bechtle.translation",
    "cape-adapter-service-retry-1",
    "trk-router-service",
    "kube-dev-public3",
    "kube-dev-public1",
    "kube-dev-public2",
    "com.bechtle.beam",
    "com.bechtle.price.v3.tk4002",
    "com.bechtle.indexer.ts",
    "com.bechtle.price.v3.tk4003",
    "com.bechtle.best.ts",
    "com.bechtle.price.v3.tabi",
    "vendor-invoice-service",
    "com.bechtle.keycloak",
    "cape-adapter-service-dlt",
    "vendor-data-management",
    "com.bechtle.bens.new",
    "com.bechtle.product",
    "com.bechtle.apache-camel-integrations",
    "com.bechtle.find"
];
function dothis() {
    return __awaiter(this, void 0, void 0, function* () {
        for (let i = 0; i < 1; i++) {
            yield connectToKafka(groups[i]);
            //await readTopics();
            yield fetchKafkaOffset(groups[i]);
            yield disconnectFromKafka();
        }
    });
}
dothis();
