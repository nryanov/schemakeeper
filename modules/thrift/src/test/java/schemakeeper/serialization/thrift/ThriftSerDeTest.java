package schemakeeper.serialization.thrift;


import org.junit.Test;
import schemakeeper.client.MockSchemaKeeperClient;
import schemakeeper.exception.ThriftDeserializationException;
import schemakeeper.exception.ThriftSerializationException;
import schemakeeper.generated.thrift.*;
import schemakeeper.schema.CompatibilityType;


import static org.junit.Assert.*;

public class ThriftSerDeTest {
    @Test
    public void simpleSerializationTest() throws ThriftSerializationException, ThriftDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.NONE);
        ThriftSerializer serializer = new ThriftSerializer(client);
        ThriftDeserializer deserializer = new ThriftDeserializer(client);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        byte[] result = serializer.serialize("test", msgV1);
        Object d = deserializer.deserialize(result);

        assertEquals(msgV1, d);
    }

    @Test
    public void throwErrorDueToSchemaIncompatibility() throws ThriftSerializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.BACKWARD);
        ThriftSerializer s1 = new ThriftSerializer(client);
        ThriftSerializer s2 = new ThriftSerializer(client);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        ThriftMsgV2 msgV2 = new ThriftMsgV2(1);

        s1.serialize("test", msgV1);

        assertThrows(RuntimeException.class, () -> s2.serialize("test", msgV2));
    }

    @Test
    public void readDataUsingOldSchema() throws ThriftSerializationException, ThriftDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.FULL);
        ThriftSerializer s1 = new ThriftSerializer(client);
        ThriftSerializer s2 = new ThriftSerializer(client);
        ThriftDeserializer deserializer = new ThriftDeserializer(client);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        s1.serialize("test", msgV1);

        ThriftMsgV5 msgV5 = new ThriftMsgV5("f1");
        byte[] result = s2.serialize("test", msgV5);

        Object d = deserializer.deserialize(result, ThriftMsgV1.class);

        assertEquals(msgV1, d);
    }

    @Test
    public void readDataWithoutSpecifiedSchema() throws ThriftSerializationException, ThriftDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.FULL);
        ThriftSerializer s1 = new ThriftSerializer(client);
        ThriftSerializer s2 = new ThriftSerializer(client);
        ThriftDeserializer deserializer = new ThriftDeserializer(client);

        ThriftMsgV1 msgV1 = new ThriftMsgV1("f1");
        s1.serialize("test", msgV1);

        ThriftMsgV5 msgV5 = new ThriftMsgV5("f1");
        byte[] result = s2.serialize("test", msgV5);

        Object d = deserializer.deserialize(result);

        assertEquals(d.getClass(), ThriftMsgV5.class);
        assertEquals(msgV5, d);
    }
}
