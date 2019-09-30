package schemakeeper.client;

import kong.unirest.Unirest;
import org.apache.avro.Schema;
import schemakeeper.api.SchemaId;
import schemakeeper.api.SchemaText;
import schemakeeper.api.SubjectAndSchemaRequest;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Optional;

public class DefaultSchemaKeeperClient extends SchemaKeeperClient {
    public DefaultSchemaKeeperClient(SerDeConfig config) {
        super(config);
        Unirest.config()
                .socketTimeout(500)
                .connectTimeout(1000)
                .concurrency(5, 2)
                .setDefaultHeader("Accept", "application/json")
                .followRedirects(false)
                .enableCookieManagement(false);
    }

    @Override
    public Optional<Schema> getSchemaById(int id) {
        logger.info("Get schema by id: {}", id);

        SchemaText schemaText = Unirest.get(SCHEMAKEEPER_URL + "/v1/schemas/" + id)
                .asObject(SchemaText.class)
                .getBody();

        if (schemaText == null) {
            return Optional.empty();
        }

        return Optional.of(schemaText.getSchema());
    }

    @Override
    public Optional<Integer> registerNewSchema(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        logger.info("Get schema id ({}) or register new schema and add to subject: {}", schema.toString(), subject);

        SchemaId schemaId = Unirest.put(SCHEMAKEEPER_URL + "/v1/subjects/" + subject + "/schemas")
                .body(SubjectAndSchemaRequest.instance(schema.toString(), schemaType, compatibilityType))
                .asObject(SchemaId.class)
                .getBody();

        if (schemaId == null) {
            return Optional.empty();
        }

        return Optional.of(schemaId.getSchemaId());
    }

    @Override
    public Optional<Integer> getSchemaId(String subject, Schema schema, SchemaType schemaType) {
        logger.info("Get schema id ({}) subject: {}", schema.toString(), subject);

        SchemaId schemaId = Unirest.post(SCHEMAKEEPER_URL + "/v1/subjects/" + subject + "/schemas")
                .body(SchemaText.instance(schema, schemaType))
                .asObject(SchemaId.class)
                .getBody();

        if (schemaId == null) {
            return Optional.empty();
        }

        return Optional.of(schemaId.getSchemaId());
    }

    @Override
    public void close() {
        Unirest.shutDown();
    }
}
