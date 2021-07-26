package schemakeeper.schema.thrift;

import org.junit.Test;
import org.apache.avro.Schema;
import schemakeeper.generated.thrift.*;

import static org.junit.Assert.*;

public class SchemaKeeperThriftDataTest {
    @Test
    public void getSchemaWithOptionalFields() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(ThriftMsgV4.class);

        String expected = "{\"type\":\"record\",\"name\":\"ThriftMsgV4\",\"namespace\":\"schemakeeper.generated.thrift\",\"fields\":[{\"name\":\"f1\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]},{\"name\":\"f3\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]},{\"name\":\"f4\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}";
        assertEquals(schema.toString(), expected);
    }

    @Test
    public void getSchemaWithDefaultValues() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);

        String expected = "{\"type\":\"record\",\"name\":\"ThriftMsgV1\",\"namespace\":\"schemakeeper.generated.thrift\",\"fields\":[{\"name\":\"f1\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]},{\"name\":\"f2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}";
        assertEquals(schema.toString(), expected);
    }

    @Test
    public void complexStructTest() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(schemakeeper.generated.thrift.Test.class);

        Schema intOptional = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        stringSchema.addProp("avro.java.string", "String");
        stringSchema = Schema.createUnion(stringSchema, Schema.create(Schema.Type.NULL));

        Schema optionalString = Schema.create(Schema.Type.STRING);
        optionalString.addProp("avro.java.string", "String");
        optionalString = Schema.createUnion(Schema.create(Schema.Type.NULL), optionalString);

        Schema longSchema = Schema.create(Schema.Type.LONG);

        assertEquals(schema.getField("i32Field").schema(), intOptional);
        assertEquals(schema.getField("stringField").schema(), stringSchema);
        assertEquals(schema.getField("stringOptionalFieldWithDefault").schema(), optionalString);
        assertEquals(schema.getField("i64Field").schema(), longSchema);
    }
}
