const { Kafka } = require("kafkajs");

async function createTopic() {
  //Admin Stuff
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["localhost:9092"],
    });

    const admin = kafka.admin();
    console.log("Kafka Broker'a bağlanılıyor...");
    await admin.connect();
    console.log("Kafka Broker'a bağlantı başarılı, Topic üretilecek..");
    await admin.createTopics({
      topics: [
        {
          topic: "pub_sub_topic",
          numPartitions: 1,
        },
      ],
    });
    console.log("Topic Başarılı Bir Şekilde Oluşturulmuştur.");
    await admin.disconnect();
  } catch (error) {
    console.log("Bir Hata Oluştu : ", error);
  } finally {
    process.exit(0);
  }
}

createTopic();
