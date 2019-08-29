package schemakeeper.serialization.thrift;


import org.apache.avro.Schema;
import org.apache.avro.thrift.ThriftData;
import org.junit.jupiter.api.Test;
import schemakeeper.client.InMemorySchemaKeeperClient;
import schemakeeper.exception.ThriftDeserializationException;
import schemakeeper.exception.ThriftSerializationException;
import schemakeeper.schema.AvroSchemaCompatibility;
import schemakeeper.schema.thrift.SchemaKeeperThriftData;
import schemakeeper.serialization.thrift.test.ThriftMsgV1;
import schemakeeper.serialization.thrift.test.ThriftMsgV2;
import schemakeeper.serialization.thrift.test.ThriftMsgV3;
import schemakeeper.serialization.thrift.test.ThriftMsgV5;

import static org.junit.jupiter.api.Assertions.*;

public class ThriftSerDeTest {
    @Test
    public void simpleSerializationTest() throws ThriftSerializationException, ThriftDeserializationException {
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("none");
        ThriftSerializer<ThriftMsgV1> serializer = new ThriftSerializer<>(client, ThriftMsgV1.class);
        ThriftDeserializer<ThriftMsgV1> deserializer = new ThriftDeserializer<>(client);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        byte[] result = serializer.serialize("test", msgV1);
        Object d = deserializer.deserialize(result);

        assertEquals(msgV1, d);
    }

    @Test
    public void throwErrorDueToSchemaIncompatibility() throws ThriftSerializationException {
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("backward");
        ThriftSerializer<ThriftMsgV1> s1 = new ThriftSerializer<>(client, ThriftMsgV1.class);
        ThriftSerializer<ThriftMsgV2> s2 = new ThriftSerializer<>(client, ThriftMsgV2.class);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        ThriftMsgV2 msgV2 = new ThriftMsgV2(1);

        s1.serialize("test", msgV1);

        assertThrows(RuntimeException.class, () -> s2.serialize("test", msgV2));
    }

    @Test
    public void readDataUsingOldSchema() throws ThriftSerializationException, ThriftDeserializationException {
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("full");
        ThriftSerializer<ThriftMsgV1> s1 = new ThriftSerializer<>(client, ThriftMsgV1.class);
        ThriftSerializer<ThriftMsgV5> s2 = new ThriftSerializer<>(client, ThriftMsgV5.class);
        ThriftDeserializer<ThriftMsgV1> deserializer = new ThriftDeserializer<>(client, ThriftMsgV1.class);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        s1.serialize("test", msgV1);

        ThriftMsgV5 msgV5 = new ThriftMsgV5("f1");
        byte[] result = s2.serialize("test", msgV5);

        Object d = deserializer.deserialize(result);
        System.out.println(d);

        assertEquals(msgV1, d);
    }
}
