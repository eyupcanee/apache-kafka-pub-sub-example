const { Kafka } = require("kafkajs");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    console.log("Producer'a bağlanılıyor...");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı.");

    const message_result = await producer.send({
      topic: "pub_sub_topic",
      messages: [
        {
          value: "Pub Sub Icerigi",
          partition: 0,
        },
      ],
    });

    console.log("Gonderim islemi basarılıdır", JSON.stringify(message_result));
    await producer.disconnect();
  } catch (error) {
    console.log("Bir Hata Oluştu : ", error);
  } finally {
    process.exit(0);
  }
}
