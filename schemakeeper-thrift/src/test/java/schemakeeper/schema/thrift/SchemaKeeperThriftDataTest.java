package schemakeeper.schema.thrift;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import schemakeeper.serialization.thrift.test.*;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaKeeperThriftDataTest {
    @Test
    public void getSchemaWithOptionalFields() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(ThriftMsgV4.class);

        String expected = "{\"type\":\"record\",\"name\":\"ThriftMsgV4\",\"namespace\":\"schemakeeper.serialization.thrift.test\",\"fields\":[{\"name\":\"f1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"f3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"f4\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}";
        assertEquals(schema.toString(), expected);
    }

    @Test
    public void getSchemaWithDefaultValues() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);

        String expected = "{\"type\":\"record\",\"name\":\"ThriftMsgV1\",\"namespace\":\"schemakeeper.serialization.thrift.test\",\"fields\":[{\"name\":\"f1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"f2\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"],\"default\":\"test\"}]}";
        assertEquals(schema.toString(), expected);
    }

    @Test
    public void complexStructTest() {
        Schema schema = SchemaKeeperThriftData.get().getSchema(schemakeeper.serialization.thrift.test.Test.class);

        Schema intOptional = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        stringSchema.addProp("avro.java.string", "String");
        stringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), stringSchema);

        Schema optionalStringSchemaWithDefaultValue = Schema.create(Schema.Type.STRING);
        optionalStringSchemaWithDefaultValue.addProp("avro.java.string", "String");
        optionalStringSchemaWithDefaultValue = Schema.createUnion(optionalStringSchemaWithDefaultValue, Schema.create(Schema.Type.NULL));

        Schema longSchema = Schema.create(Schema.Type.LONG);

        assertEquals(schema.getField("i32Field").schema(), intOptional);
        assertEquals(schema.getField("stringField").schema(), stringSchema);
        assertEquals(schema.getField("stringOptionalFieldWithDefault").schema(), optionalStringSchemaWithDefaultValue);
        assertEquals(schema.getField("i64Field").schema(), longSchema);
    }
}
