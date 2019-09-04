package schemakeeper.serialization;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.Map;

public class AvroSerDeConfig extends SerDeConfig {
    public static final String USE_SPECIFIC_READER_CONFIG = "use.specific.reader";
    public static final String SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG = "specific.reader.schema.per.subject";

    public AvroSerDeConfig(Map<String, Object> config) {
        super(config);
    }


    public boolean useSpecificReader() {
        return (boolean) config.getOrDefault(USE_SPECIFIC_READER_CONFIG, false);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Schema> specificReaderPerSubjectConfig() {
        return (Map<String, Schema>) config.getOrDefault(SPECIFIC_READER_SCHEMA_PER_SUBJECT_CONFIG, Collections.EMPTY_MAP);
    }
}
