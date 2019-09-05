package schemakeeper.kafka.serialization.protobuf

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.protobuf.ProtobufData
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.scalatest.WordSpec
import schemakeeper.client.MockSchemaKeeperClient
import schemakeeper.serialization.protobuf.test.Message
import schemakeeper.serialization.protobuf.{ProtobufDeserializer, ProtobufSerializer}

import scala.collection.JavaConverters._


class KafkaProtobufSerDeTest extends WordSpec with EmbeddedKafka {
  val config = new KafkaProtobufSerDeConfig(Map.empty[String, AnyRef].asJava)
  val client = new MockSchemaKeeperClient(config)

  implicit val byteSerializer = new ByteArraySerializer()
  implicit val byteDeserializer = new ByteArrayDeserializer()

  "KafkaProtobufSerDe" should {
    "serialize and deserialize message correctly" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit cfg =>
        val serializer = new KafkaProtobufSerializer(new ProtobufSerializer(client, config))
        val deserializer = new KafkaProtobufDeserializer(new ProtobufDeserializer(client, config))
        val record = Message.ProtoMsgV1.newBuilder().setF1("f1").setF2("f2").build()
        val schema = ProtobufData.get().getSchema(classOf[Message.ProtoMsgV1])

        publishToKafka[Array[Byte]]("test", serializer.serialize("test", record))
        val msg: Array[Byte] = consumeFirstMessageFrom[Array[Byte]]("test")

        assertResult(record)(deserializer.deserialize("test", msg))
        assertResult(1)(client.getId)
        assertResult(schema)(client.getIdSchema.get(1))
      }
    }
  }
}
