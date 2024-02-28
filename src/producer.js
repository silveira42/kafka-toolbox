const {Kafka} = require('kafkajs')

const topicName = process.argv[2];
const key = process.argv[3];
const message = process.argv[4];

const kafka = new Kafka({
	'clientId': process.env.KAFKA_CLIENT_ID,
	'brokers' :[ process.env.KAFKA_HOST + ':' + process.env.KAFKA_PORT ]
});

sendMessage(topicName, key, message)

async function sendMessage(topicName, key, message){
	try {
		if (topicName == '') {
			throw new Error ('topicName is mandatory.')
		}

		const producer = kafka.producer();

		console.log('Connecting...');
		await producer.connect();
		console.log('Connected!');

		const result =  await producer.send({
			'topic': topicName,
			'messages': [
				{
					'key': key,
					'value': message
				}
			]
		})

		console.log(`Send Successfully! ${JSON.stringify(result)}`)

		await producer.disconnect();
	} catch(exception) {
		console.error(`Something bad happened ${exception}`);
	} finally {
		process.exit(0);
	}
}
