package schemakeeper.kafka.naming;

import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Returns fully qualified record name as subject name.
 * Supposed to work only with record schema type. Otherwise throws error.
 */
public class RecordNamingStrategy implements NamingStrategy {
    @Override
    public String resolveSubjectName(String topicName, boolean isKey, Schema schema) {
        if (schema != null) {
            if (schema.getType() != Schema.Type.RECORD) {
                throw new SerializationException(getClass().getName() + ": the message must be an Avro Record");
            } else {
                return schema.getFullName();
            }
        } else {
            return null;
        }
    }
}
