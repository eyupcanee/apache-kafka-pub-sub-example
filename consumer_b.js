const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "consumer_group_b",
    });
    console.log("Consumer'a bağlanılıyor...");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı.");

    //Consumer Subscribe
    await consumer.subscribe({
      topic: "pub_sub_topic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(`Gelen Mesaj ${result.message.value} From Group B`);
      },
    });
  } catch (error) {
    console.log("Bir Hata Oluştu : ", error);
  }
}
