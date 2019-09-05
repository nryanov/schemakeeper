package schemakeeper.kafka.serialization.thrift

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.scalatest.WordSpec
import schemakeeper.client.MockSchemaKeeperClient
import schemakeeper.schema.thrift.SchemaKeeperThriftData
import schemakeeper.serialization.thrift.test.ThriftMsgV1
import schemakeeper.serialization.thrift.{ThriftDeserializer, ThriftSerializer}

import scala.collection.JavaConverters._

class KafkaThriftSerDeTest extends WordSpec with EmbeddedKafka {
  val config = new KafkaThriftSerDeConfig(Map.empty[String, AnyRef].asJava)
  val client = new MockSchemaKeeperClient(config)

  implicit val byteSerializer = new ByteArraySerializer()
  implicit val byteDeserializer = new ByteArrayDeserializer()

  "KafkaAvroSerDe" should {
    "serialize and deserialize message correctly" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit cfg =>
        val serializer = new KafkaThriftSerializer(new ThriftSerializer(client, config))
        val deserializer = new KafkaThriftDeserializer(new ThriftDeserializer(client, config))
        val record = new ThriftMsgV1().setF1("f1").setF2("f2")
        val schema = SchemaKeeperThriftData.get().getSchema(classOf[ThriftMsgV1])

        publishToKafka[Array[Byte]]("test", serializer.serialize("test", record))
        val msg: Array[Byte] = consumeFirstMessageFrom[Array[Byte]]("test")

        assertResult(record)(deserializer.deserialize("test", msg))
        assertResult(1)(client.getId)
        assertResult(schema)(client.getIdSchema.get(1))
      }
    }
  }
}
