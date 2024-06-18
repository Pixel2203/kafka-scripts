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
const { Kafka } = require('kafkajs');
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['b001cvs000298.bechtle.de:9092']
});
const admin = kafka.admin();
// @ts-ignore
const listConsumerGroupTopics = (groups) => __awaiter(void 0, void 0, void 0, function* () {
    yield admin.connect();
    try {
        const alleTopicsDieGelesenWerdenInKafka = new Set();
        for (const group of groups) {
            console.log(`\nConsumer Group: ${group}`);
            // Hole die Beschreibungen der Consumer-Gruppe
            const groupDescription = yield admin.describeGroups([group]);
            // Extrahiere Topics aus d en Offsets und Beschreibungen
            groupDescription.groups.map((gruppe) => {
                const consumerInGruppe = gruppe.members;
                consumerInGruppe.forEach((member) => {
                    //console.log(`ClientId: ${member.clientId}`)
                    //console.log(`MemberId: ${member.memberId}`)
                    const memberMetadata = kafkajs_1.AssignerProtocol.MemberMetadata.decode(member.memberMetadata);
                    console.log(`Topics: `);
                    if (memberMetadata) {
                        memberMetadata.topics.forEach(topic => alleTopicsDieGelesenWerdenInKafka.add(topic));
                    }
                    memberMetadata === null || memberMetadata === void 0 ? void 0 : memberMetadata.topics.forEach(topic => {
                        // console.log(` - ${topic}`)
                    });
                });
            });
        }
        //console.log("ALLE TOPICS DIE GELESEN WERDEN:")
        alleTopicsDieGelesenWerdenInKafka.forEach(topic => {
            //console.log(`- ${topic}`)
        });
        const alleTopicsInKafka = yield kafka.admin().listTopics();
        console.log(alleTopicsInKafka);
        console.log("Ungelesene Topic");
        alleTopicsInKafka.forEach((topic) => {
            if (!alleTopicsDieGelesenWerdenInKafka.has(topic)) {
                console.log(" NICHT GELESEN : " + topic);
            }
        });
    }
    finally {
        yield admin.disconnect();
    }
});
// Liste der Consumer-Gruppen
const consumerGroups = [
    'com.bechtle.price.v3',
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
listConsumerGroupTopics(consumerGroups).catch(console.error);
