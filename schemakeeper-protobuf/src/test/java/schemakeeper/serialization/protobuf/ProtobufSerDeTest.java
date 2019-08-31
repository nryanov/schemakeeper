package schemakeeper.serialization.protobuf;

import org.junit.jupiter.api.Test;
import schemakeeper.client.InMemorySchemaKeeperClient;
import schemakeeper.exception.ProtobufDeserializationException;
import schemakeeper.exception.ProtobufSerializationException;
import schemakeeper.serialization.protobuf.test.Message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufSerDeTest {
    @Test
    public void simpleSerializationTest() throws ProtobufSerializationException, ProtobufDeserializationException {
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("none");
        ProtobufSerializer<Message.ProtoMsgV1> serializer = new ProtobufSerializer<>(client, Message.ProtoMsgV1.class);
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
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("backward");
        ProtobufSerializer<Message.ProtoMsgV1> s1 = new ProtobufSerializer<>(client, Message.ProtoMsgV1.class);
        ProtobufSerializer<Message.ProtoMsgV2> s2 = new ProtobufSerializer<>(client, Message.ProtoMsgV2.class);

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
        InMemorySchemaKeeperClient client = new InMemorySchemaKeeperClient("full");
        ProtobufSerializer<Message.ProtoMsgV1> s1 = new ProtobufSerializer<>(client, Message.ProtoMsgV1.class);
        ProtobufSerializer<Message.ProtoMsgV4> s2 = new ProtobufSerializer<>(client, Message.ProtoMsgV4.class);
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
}
