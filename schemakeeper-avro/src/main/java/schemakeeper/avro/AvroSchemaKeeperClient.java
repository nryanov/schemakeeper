package schemakeeper.avro;

import org.apache.avro.Schema;

public interface AvroSchemaKeeperClient {
    Schema getSchemaById(int id);

    Iterable<Schema> getSubjectSchemas(String subject);

    Iterable<String> getSubjects();

    int getSchemaId(String subject, Schema schema);

    Schema getLastSubjectSchema(String subject);

    int registerNewSchema(String subject, Schema schema);

    boolean deleteSchemaById(int id);
}
