package schemakeeper.schema.protobuf;

import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.junit.jupiter.api.Test;
import schemakeeper.schema.AvroSchemaCompatibility;
import schemakeeper.serialization.protobuf.test.Message;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtobufSchemaCompatibilityTest {
    // proto3 supports only optional fields, so the only possible non-compatible change is field type change.

    @Test
    public void noneCompatibility() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV2.class);

        //always true
        assertTrue(AvroSchemaCompatibility.NONE_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void backwardCompatibilityTrue() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV3.class);

        assertTrue(AvroSchemaCompatibility.BACKWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void backwardCompatibilityFalse() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV2.class);

        assertFalse(AvroSchemaCompatibility.BACKWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void forwardCompatibilityTrue() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV3.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV4.class);

        assertTrue(AvroSchemaCompatibility.FORWARD_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void forwardCompatibilityFalse() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV2.class);

        assertFalse(AvroSchemaCompatibility.FORWARD_VALIDATOR().isCompatible(s2, s1));
    }


    @Test
    public void fullCompatibilityTrue() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV4.class);

        assertTrue(AvroSchemaCompatibility.FULL_VALIDATOR().isCompatible(s2, s1));
    }

    @Test
    public void fullCompatibilityFalse() {
        Schema s1 = ProtobufData.get().getSchema(Message.ProtoMsgV1.class);
        Schema s2 = ProtobufData.get().getSchema(Message.ProtoMsgV2.class);

        assertFalse(AvroSchemaCompatibility.FULL_VALIDATOR().isCompatible(s2, s1));
    }
}
