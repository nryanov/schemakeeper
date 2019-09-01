package schemakeeper.kafka;


public interface NamingStrategy {
    String resolveSubjectName(String topicName, boolean isKey);
}
