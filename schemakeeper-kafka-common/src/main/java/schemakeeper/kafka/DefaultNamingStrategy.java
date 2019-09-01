package schemakeeper.kafka;

import org.apache.avro.Schema;

public class DefaultNamingStrategy implements NamingStrategy {
    public static NamingStrategy INSTANCE = new DefaultNamingStrategy();

    @Override
    public String resolveSubjectName(String topicName, boolean isKey) {
        if (isKey) {
            return topicName + "-key";
        } else {
            return topicName + "-value";
        }
    }
}
