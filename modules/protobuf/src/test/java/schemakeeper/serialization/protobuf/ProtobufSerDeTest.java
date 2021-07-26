package schemakeeper.serialization.protobuf;

import org.junit.Test;
import schemakeeper.client.MockSchemaKeeperClient;
import schemakeeper.exception.ProtobufDeserializationException;
import schemakeeper.exception.ProtobufSerializationException;
import schemakeeper.generated.protobuf.Message;
import schemakeeper.schema.CompatibilityType;

import static org.junit.Assert.*;


public class ProtobufSerDeTest {
    @Test
    public void simpleSerializationTest() throws ProtobufSerializationException, ProtobufDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.NONE);
        ProtobufSerializer serializer = new ProtobufSerializer(client);
        ProtobufDeserializer deserializer = new ProtobufDeserializer(client);

        Message.ProtoMsgV1 msgV1 = Message.ProtoMsgV1
                .newBuilder()
                .setF1("f1")
                .setF2("f2")
                .build();

        byte[] result = serializer.serialize("test", msgV1);
        Object d = deserializer.deserialize(result);

        assertEquals(msgV1, d);
    }

    @Test
    public void throwErrorDueToSchemaIncompatibility() throws ProtobufSerializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.BACKWARD);
        ProtobufSerializer s1 = new ProtobufSerializer(client);
        ProtobufSerializer s2 = new ProtobufSerializer(client);

        Message.ProtoMsgV1 msgV1 = Message.ProtoMsgV1
                .newBuilder()
                .setF1("f1")
                .setF2("f2")
                .build();

        Message.ProtoMsgV2 msgV2 = Message.ProtoMsgV2
                .newBuilder()
                .setF1(1)
                .build();

        s1.serialize("test", msgV1);

        assertThrows(RuntimeException.class, () -> s2.serialize("test", msgV2));
    }

    @Test
    public void readDataUsingOldSchema() throws ProtobufSerializationException, ProtobufDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.FULL);
        ProtobufSerializer s1 = new ProtobufSerializer(client);
        ProtobufSerializer s2 = new ProtobufSerializer(client);
        ProtobufDeserializer deserializer = new ProtobufDeserializer(client);

        Message.ProtoMsgV1 msgV1 = Message.ProtoMsgV1
                .newBuilder()
                .setF1("f1")
                .clearF2() // set as null
                .build();

        s1.serialize("test", msgV1);

        Message.ProtoMsgV4 msgV4 = Message.ProtoMsgV4
                .newBuilder()
                .setF1("f1")
                .setF3("f3")
                .setF4("f4")
                .build();
        byte[] result = s2.serialize("test", msgV4);

        Object d = deserializer.deserialize(result, Message.ProtoMsgV1.class);

        assertEquals(msgV1, d);
    }

    @Test
    public void readDataWithoutSpecifiedSchema() throws ProtobufSerializationException, ProtobufDeserializationException {
        MockSchemaKeeperClient client = new MockSchemaKeeperClient(CompatibilityType.FULL);
        ProtobufSerializer s1 = new ProtobufSerializer(client);
        ProtobufSerializer s2 = new ProtobufSerializer(client);
        ProtobufDeserializer deserializer = new ProtobufDeserializer(client);

        Message.ProtoMsgV1 msgV1 = Message.ProtoMsgV1
                .newBuilder()
                .setF1("f1")
                .clearF2() // set as null
                .build();

        s1.serialize("test", msgV1);

        Message.ProtoMsgV4 msgV4 = Message.ProtoMsgV4
                .newBuilder()
                .setF1("f1")
                .setF3("f3")
                .setF4("f4")
                .build();
        byte[] result = s2.serialize("test", msgV4);

        Object d = deserializer.deserialize(result);

        assertEquals(d.getClass(), Message.ProtoMsgV4.class);
        assertEquals(msgV4, d);
    }
}
