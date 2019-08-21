package schemakeeper.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import schemakeeper.avro.test.Message;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class AvroSerDeTest {
    private InMemorySchemaKeeperClient client;
    private AvroSerializer serializer;
    private AvroDeserializer deserializer;
    private AvroSerDeConfig config;

    @BeforeEach
    public void set() {
        this.client = new InMemorySchemaKeeperClient();
        this.config = new AvroSerDeConfig(Collections.emptyMap());
        this.serializer = new AvroSerializer(client, config);
        this.deserializer = new AvroDeserializer(client, config);
    }

    @Test
    public void byteArrayData() throws IOException {
        byte[] data = "some data".getBytes(StandardCharsets.UTF_8);
        byte[] s = serializer.serialize("test", data);
        byte[] d = (byte[]) deserializer.deserialize(s);
        assertArrayEquals(data, d);
    }

    @Test
    public void byteData() throws IOException {
        byte data = 0x1;
        byte[] s = serializer.serialize("test", data);
        int d = (int) deserializer.deserialize(s); // returned data will be an Integer
        assertEquals(data, d);
    }

    @Test
    public void shortData() throws IOException {
        short data = 1;
        byte[] s = serializer.serialize("test", data);
        int d = (int) deserializer.deserialize(s); // returned data will be an Integer
        assertEquals(data, d);
    }

    @Test
    public void intData() throws IOException {
        int data = 1;
        byte[] s = serializer.serialize("test", data);
        int d = (int) deserializer.deserialize(s);
        assertEquals(data, d);
    }

    @Test
    public void longData() throws IOException {
        long data = 1L;
        byte[] s = serializer.serialize("test", data);
        long d = (long) deserializer.deserialize(s);
        assertEquals(data, d);
    }

    @Test
    public void floatData() throws IOException {
        float data = 1.0f;
        byte[] s = serializer.serialize("test", data);
        float d = (float) deserializer.deserialize(s);
        assertEquals(data, d);
    }

    @Test
    public void doubleData() throws IOException {
        double data = 1.0D;
        byte[] s = serializer.serialize("test", data);
        double d = (double) deserializer.deserialize(s);
        assertEquals(data, d);
    }

    @Test
    public void stringData() throws IOException {
        String data = "1";
        byte[] s = serializer.serialize("test", data);
        String d = (String) deserializer.deserialize(s);
        assertEquals(data, d);
    }

    @Test
    public void nullData() throws IOException {
        Object data = null;
        byte[] s = serializer.serialize("test", data);
        Object d = deserializer.deserialize(s);
        assertNull(d);
    }

    @Test
    public void genericData() throws IOException {
        Schema schema = SchemaBuilder.record("test")
                .fields()
                .requiredString("f")
                .endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("f", "some value");

        byte[] result = serializer.serialize("test", record);
        Object d = deserializer.deserialize(result);

        assertEquals(record, d);

    }

    @Test
    public void specificData() throws IOException {
        config = new AvroSerDeConfig(Collections.singletonMap(AvroSerDeConfig.USE_SPECIFIC_READER_CONFIG, true));
        serializer = new AvroSerializer(client, config);
        deserializer = new AvroDeserializer(client, config);

        Message message = Message.newBuilder()
                .setF1(1)
                .setF2("2")
                .setF3(3)
                .setF4(null)
                .build();

        byte[] result = serializer.serialize("test", message);
        Object d = deserializer.deserialize(result);

        assertEquals(message, d);
    }
}
