const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["192.168.8.148:9092"]
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group"
    });

    console.log("Connecting to Consumer...");
    await consumer.connect();
    console.log("Connected successfully");

    // Consumer Subscribe..
    await consumer.subscribe({
      topic: "LogStoreTopic",
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async result => {
        console.log(
          `Message received ${result.message.value}, Par => ${result.partition}`
        );
      }
    });
  } catch (error) {
    console.log("Error", error);
  }
}
