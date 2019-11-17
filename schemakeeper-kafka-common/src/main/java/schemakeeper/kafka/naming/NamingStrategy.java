package schemakeeper.kafka.naming;


import org.apache.avro.Schema;

public interface NamingStrategy {
    /**
     *
     * @param topicName - kafka's topic name
     * @param isKey - flag indicates that this schema is for key or not
     * @param schema - avro schema
     * @return - subject name
     */
    String resolveSubjectName(String topicName, boolean isKey, Schema schema);
}
