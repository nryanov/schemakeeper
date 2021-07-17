package schemakeeper.kafka.naming;

import org.apache.avro.Schema;

/**
 * Returns topic name concatenated with fully qualified record name as subject name.
 * Supposed to work only with record schema type. Otherwise throws error.
 */
public class TopicWithRecordNamingStrategy extends RecordNamingStrategy {
    @Override
    public String resolveSubjectName(String topicName, boolean isKey, Schema schema) {
        if (schema == null) {
            return null;
        } else {
            return topicName + "-" + super.resolveSubjectName(topicName, isKey, schema);
        }
    }
}
