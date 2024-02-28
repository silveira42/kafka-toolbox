
//const Kafka = require('kafkajs').Kafka
const {Kafka} = require('kafkajs')

const topicName = process.argv[2];
const groupId = process.argv[3];

const kafka = new Kafka({
	'clientId': process.env.KAFKA_CLIENT_ID,
	'brokers' :[ process.env.KAFKA_HOST + ':' + process.env.KAFKA_PORT ]
});

listen(topicName, groupId);

async function listen(topicName, groupId){
	try {
		const consumer = kafka.consumer({"groupId": groupId});
		console.log('Connecting...');
		await consumer.connect();
		console.log('Connected!');

		await consumer.subscribe({
			"topic": topicName,
			"fromBeginning": true
		})

		await consumer.run({
			"eachMessage": async (result) => {
				console.log(`${topicName} - ${result.message.key} - Message ${result.message.value} on partition ${result.partition}`)
			}
		});

	} catch(exception) {
		console.error(`Something bad happened ${exception}`);
	}
}
