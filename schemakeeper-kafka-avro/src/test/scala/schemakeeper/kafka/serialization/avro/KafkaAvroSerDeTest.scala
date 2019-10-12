package schemakeeper.kafka.serialization.avro

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.scalatest.WordSpec
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.client.MockSchemaKeeperClient
import schemakeeper.serialization.SerDeConfig
import schemakeeper.serialization.avro.{AvroDeserializer, AvroSerializer}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KafkaAvroSerDeTest extends WordSpec with EmbeddedKafka {
  val config = new KafkaAvroSerDeConfig(Map[String, AnyRef](SerDeConfig.SCHEMAKEEPER_URL_CONFIG -> "mock").asJava)
  val client = new MockSchemaKeeperClient(config)

  implicit val byteSerializer = new ByteArraySerializer()
  implicit val byteDeserializer = new ByteArrayDeserializer()

  "KafkaAvroSerDe" should {
    "serialize and deserialize message correctly" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit cfg =>
        val serializer = new KafkaAvroSerializer(new AvroSerializer(client, config))
        val deserializer = new KafkaAvroDeserializer(new AvroDeserializer(client, config))
        val schema = SchemaBuilder
          .builder()
          .record("test")
          .fields()
          .requiredString("f1")
          .optionalString("f2")
          .nullableString("f3", "value")
          .endRecord()

        val record = new GenericData.Record(schema)
        record.put("f1", "f1")

        publishToKafka[Array[Byte]]("test", serializer.serialize("test", record))
        val msg: Array[Byte] = consumeFirstMessageFrom[Array[Byte]]("test")

        assertResult(record)(deserializer.deserialize("test", msg))
        assertResult(1)(client.getId)
        assertResult(schema)(client.getIdSchema.get(1))
      }
    }
  }

}
