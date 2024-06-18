import {Kafka, logLevel, Partitioners} from "kafkajs";

//const app = require("express")()
//app.listen(9000, () => console.log("Listening to port 9000"))
const clientId = "myProducerId"

// Setting up Kafka
const brokers = ["127.0.0.1:9092"];
const kafka = new Kafka({
    logLevel: logLevel.ERROR,
    brokers: brokers,
    clientId: clientId,
})
console.log("Meine ClientID: " + clientId)
const times:number = Number(process.argv[2]);
const topic = process.argv[3];

async function connectToKafka() {
    const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner});
    await producer.connect();
    return producer;
}


async function produceMessages() {
    const producer = await connectToKafka();
    for(let i  = 0; i <times; i++){
        const msg = generateMessage();
        console.log("Generated Message")
        console.log(msg)
        await producer.send({topic: topic,messages: Array.of(msg)})
    }
}
function generateMessage()   {
    const number = Math.floor(Math.random() * 100000)
    const message = {
        key: "myKey",
        value: JSON.stringify(
            {
                product: {
                    id: number
                }
            }


        )
    } as any;
    return message;
}

produceMessages();