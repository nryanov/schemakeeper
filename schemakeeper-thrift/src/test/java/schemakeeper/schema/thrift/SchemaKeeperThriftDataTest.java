package schemakeeper.schema.thrift;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import schemakeeper.serialization.thrift.test.*;

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
}
