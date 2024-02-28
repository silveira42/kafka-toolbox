//const Kafka = require('kafkajs').Kafka
const {Kafka} = require('kafkajs')

const action = process.argv[2];

const kafka = new Kafka({
	'clientId': process.env.KAFKA_CLIENT_ID,
	'brokers' :[ process.env.KAFKA_HOST + ':' + process.env.KAFKA_PORT ]
});

menu();

async function menu() {
	try {

		if (action == 'create') {
			const topicName = process.argv[3];
			const partitionNum = process.argv[4];

			await createTopic(topicName, partitionNum);
		} else if (action == 'list') {
			await listTopics();
		} else if (action == '-h' || action == '--help') {
			console.log('Usage: node --env-file=.env ./src/topic.js <action> <topicName> <particionNum>');
		} else if (action == '') {
			console.error('Action is mandatory!');
		} else {
			console.error('Invalid Action.');
		}

	} catch(exception) {
		console.error(`Something bad happened ${exception}`);
	}	finally {
		process.exit(0);
	}
}

async function createTopic(topicName, partitionNum) {
	try {
		const admin = kafka.admin();

		console.log('Connecting...');
		await admin.connect();
		console.log('Connected!');

		await admin.createTopics({
			'topics': [{
				'topic' : topicName,
				'numPartitions': partitionNum
			}]
		});

		console.log(`Topic ${topicName} created successfully!`);

		console.log(await admin.listTopics());

		await admin.disconnect();
	} catch(exception) {
		console.error(exception);
	} finally {
		process.exit(0);
	}
}

async function listTopics(){
	try {
		const admin = kafka.admin();
		console.log('Connecting.....');
		await admin.connect();
		console.log('Connected!');

		console.log(await admin.listTopics());

		await admin.disconnect();
	} catch(exception) {
		console.error(exception);
	}	finally {
		process.exit(0);
	}
}
