package schemakeeper.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractAvroSerDe {
    protected static final byte AVRO_BYTE = 0x1;

    protected AvroSchemaKeeperClient client;
    protected AvroSerDeConfig config;

    public AbstractAvroSerDe(AvroSchemaKeeperClient client, AvroSerDeConfig config) {
        this.client = client;
        this.config = config;
    }

    protected void writeAvroProtocolByte(ByteArrayOutputStream out) {
        out.write(AVRO_BYTE);
    }

    protected void writeAvroSchemaId(ByteArrayOutputStream out, int id) throws IOException {
        out.write(ByteBuffer.allocate(4).putInt(id).array());
    }

    protected void readAvroProtocolByte(ByteBuffer buffer) {
        if (buffer.get() != AVRO_BYTE) {
            throw new IllegalArgumentException("This is not an avro data");
        }
    }
}
