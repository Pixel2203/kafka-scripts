import {AssignerProtocol, Consumer, GroupDescription, GroupOverview, MemberDescription} from "kafkajs";

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['b001cvs000298.bechtle.de:9092']
});

const admin = kafka.admin();

// @ts-ignore
const listConsumerGroupTopics = async (groups) => {
    await admin.connect();
    try {
        const alleTopicsDieGelesenWerdenInKafka = new Set();
        //groups = await kafka.admin().listGroups();
        //roups = groups.map((group: GroupOverview) => { group.groupId})
        for (const group of groups) {
            console.log(`\nConsumer Group: ${group}`);
            // Hole die Beschreibungen der Consumer-Gruppe
            const groupDescription = await admin.describeGroups([group]);

            // Extrahiere Topics aus d en Offsets und Beschreibungen

            groupDescription.groups.map((gruppe:GroupDescription) => {
                const consumerInGruppe = gruppe.members;
                consumerInGruppe.forEach((member:MemberDescription) => {
                    //console.log(`ClientId: ${member.clientId}`)
                    //console.log(`MemberId: ${member.memberId}`)
                    const memberMetadata = AssignerProtocol.MemberMetadata.decode(member.memberMetadata)

                    console.log(`Topics: `)
                    if(memberMetadata){
                        memberMetadata.topics.forEach(topic => alleTopicsDieGelesenWerdenInKafka.add(topic))
                    }

                    memberMetadata?.topics.forEach(topic => {
                       // console.log(` - ${topic}`)
                    })


                })
            })







        }
        //console.log("ALLE TOPICS DIE GELESEN WERDEN:")
        alleTopicsDieGelesenWerdenInKafka.forEach(topic => {
            //console.log(`- ${topic}`)
        })

        const alleTopicsInKafka = await kafka.admin().listTopics();

        console.log(alleTopicsInKafka)
        console.log("Ungelesene Topic")

        alleTopicsInKafka.forEach((topic: string) => {
            if(!alleTopicsDieGelesenWerdenInKafka.has(topic)){
                console.log(" NICHT GELESEN : " + topic)
            }
        })


    } finally {
        await admin.disconnect();
    }
};

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
