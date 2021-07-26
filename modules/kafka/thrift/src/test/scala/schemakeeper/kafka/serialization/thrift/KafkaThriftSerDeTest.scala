package schemakeeper.kafka.serialization.thrift

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import schemakeeper.client.MockSchemaKeeperClient
import schemakeeper.schema.thrift.SchemaKeeperThriftData
import schemakeeper.serialization.SerDeConfig
import schemakeeper.serialization.thrift.{ThriftDeserializer, ThriftSerializer}
import schemakeeper.generated.thrift.ThriftMsgV1
import munit._

import scala.collection.JavaConverters._

class KafkaThriftSerDeTest extends FunSuite with EmbeddedKafka {
  val config = new KafkaThriftSerDeConfig(Map[String, AnyRef](SerDeConfig.SCHEMAKEEPER_URL_CONFIG -> "mock").asJava)
  val client = new MockSchemaKeeperClient(config)

  implicit val byteSerializer = new ByteArraySerializer()
  implicit val byteDeserializer = new ByteArrayDeserializer()

  test("serialize and deserialize message correctly") {
    val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

    withRunningKafkaOnFoundPort(userDefinedConfig) { implicit cfg =>
      val serializer = new KafkaThriftSerializer(new ThriftSerializer(client, config))
      val deserializer = new KafkaThriftDeserializer(new ThriftDeserializer(client, config))
      val record = new ThriftMsgV1().setF1("f1").setF2("f2")
      val schema = SchemaKeeperThriftData.get().getSchema(classOf[ThriftMsgV1])

      publishToKafka[Array[Byte]]("test", serializer.serialize("test", record))
      val msg: Array[Byte] = consumeFirstMessageFrom[Array[Byte]]("test")

      assertEquals(record, deserializer.deserialize("test", msg).asInstanceOf[ThriftMsgV1])
      assertEquals(1, client.getId)
      assertEquals(schema, client.getIdSchema.get(1))
    }
  }

}
