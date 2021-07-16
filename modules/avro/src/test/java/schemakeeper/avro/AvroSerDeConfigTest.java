package schemakeeper.avro;

import org.apache.avro.Schema;
import org.junit.Test;
import schemakeeper.serialization.avro.AvroSerDeConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AvroSerDeConfigTest {
    @Test
    public void useSpecificReader() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AvroSerDeConfig.USE_SPECIFIC_READER_CONFIG, true);

        AvroSerDeConfig avroSerDeConfig = new AvroSerDeConfig(cfg);
        AvroSerDeConfig defaultAvroSerDeConfig = new AvroSerDeConfig(Collections.emptyMap());

        assertTrue(avroSerDeConfig.useSpecificReader());
        assertFalse(defaultAvroSerDeConfig.useSpecificReader());
    }

    @Test
    public void specificReaderPerSubjectConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AvroSerDeConfig.SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG, Collections.singletonMap("fullClassName", Schema.create(Schema.Type.STRING)));

        AvroSerDeConfig avroSerDeConfig = new AvroSerDeConfig(cfg);
        AvroSerDeConfig defaultAvroSerDeConfig = new AvroSerDeConfig(Collections.emptyMap());

        assertEquals(Collections.singletonMap("fullClassName", Schema.create(Schema.Type.STRING)), avroSerDeConfig.specificReaderPerSubjectConfig());
        assertTrue(defaultAvroSerDeConfig.specificReaderPerSubjectConfig().isEmpty());
    }
}
