import {Consumer, Kafka, logLevel, Partitioners} from "kafkajs";
const ip:string = process.argv[2];
const brokers = [ip];
const clientId = "myConsumerId"
const kafka = new Kafka({
    logLevel: logLevel.ERROR,
    brokers: brokers,
    clientId: clientId,
})
var consumer: Consumer;
async function connectToKafka(groupid: string) {
    consumer = kafka.consumer({groupId: groupid});
    await consumer.connect();
}
async function disconnectFromKafka() {
    await consumer.disconnect();
}


// Suchen die Offsets für alle Topics für die Gruppen
// Schauen welche Topics aktive Consumer haben in allen gruppen
// Das in eine Liste bündeln
// Doppelte einträge entfernen
// Liste mit den original topics abgleichen
// Ergebnis: Nicht konsumierte Topics




async function fetchKafkaOffset(group: string) {
    const result = await kafka.admin().fetchOffsets({groupId: group })
    console.log("Ergebnis für Gruppe : " + group)
    console.log(result)
    const desc = await kafka.admin().describeGroups(Array.of(group))
}









async function readTopics(){
    const result = await consumer.describeGroup();
    console.log("Result:")
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
]


async function dothis() {
    for(let i = 0; i < 1; i++){
        await connectToKafka( groups[i] );
        //await readTopics();
        await fetchKafkaOffset(groups[i])
        await disconnectFromKafka();

    }
}
dothis();


