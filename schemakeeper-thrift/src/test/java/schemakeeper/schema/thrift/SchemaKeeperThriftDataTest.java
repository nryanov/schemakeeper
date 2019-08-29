package schemakeeper.schema.thrift;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import schemakeeper.schema.AvroSchemaCompatibility;
import schemakeeper.serialization.thrift.test.*;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaKeeperThriftDataTest {
    @Test
    public void noneCompatibility() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV2.class);

        //always true
        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void backwardCompatibilityTrue() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV3.class);

        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void backwardCompatibilityFalse() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV2.class);

        assertFalse(AvroSchemaCompatibility.BACKWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void forwardCompatibilityTrue() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV3.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV4.class);

        assertTrue(AvroSchemaCompatibility.FORWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void forwardCompatibilityFalse() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV2.class);

        assertFalse(AvroSchemaCompatibility.FORWARD_VALIDATOR().isCompatible(s2, s1));
    }


    @Test
    public void fullCompatibilityTrue() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV5.class);

        assertTrue(AvroSchemaCompatibility.FULL_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void fullCompatibilityFalse() {
        Schema s1 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV1.class);
        Schema s2 = SchemaKeeperThriftData.get().getSchema(ThriftMsgV2.class);

        assertFalse(AvroSchemaCompatibility.FULL_VALIDATOR().isCompatible(s2, s1));
    }
}
