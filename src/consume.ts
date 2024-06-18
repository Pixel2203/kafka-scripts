import { Kafka, logLevel, Partitioners } from "kafkajs";
const ip:string = process.argv[2];
const brokers = [ip];
const clientId = "myConsumerId"
const kafka = new Kafka({
    logLevel: logLevel.ERROR,
    brokers: brokers,
    clientId: clientId,
})
async function connectToKafka() {
    const consumer = kafka.consumer({groupId: "myconsumerd"});
    await consumer.connect();
    return consumer;
}

async function consumeMessages(){
    const consumer = await connectToKafka();
    await consumer.subscribe({topics: ["com.bechtle.mails.v2", "com.bechtle.customers.user.keycloak.v1"]})
    console.log("Subscribed to topics")
    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log("Read Message of topic " + topic)
            console.log(message.value?.toString())
        },
    })
}

consumeMessages();
