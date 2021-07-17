package schemakeeper.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import kong.unirest.Config;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.UnirestInstance;
import org.apache.avro.Schema;
import schemakeeper.api.SchemaId;
import schemakeeper.api.SchemaMetadata;
import schemakeeper.api.SchemaText;
import schemakeeper.api.SubjectAndSchemaRequest;
import schemakeeper.client.protocol.SchemaMetadataDeserializer;
import schemakeeper.client.protocol.SchemaTextSerializer;
import schemakeeper.exception.SchemaKeeperException;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.io.IOException;

public class DefaultSchemaKeeperClient extends SchemaKeeperClient {
    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();

        module.addDeserializer(SchemaMetadata.class, new SchemaMetadataDeserializer());
        module.addSerializer(SchemaText.class, new SchemaTextSerializer());

        mapper.registerModule(module);
    }

    private UnirestInstance clientInstance;

    public DefaultSchemaKeeperClient(SerDeConfig config) {
        super(config);

        clientInstance = Unirest.spawnInstance();
        Config clientInstanceConfig = clientInstance.config();

        clientInstanceConfig
                .socketTimeout(config.clientSocketTimeout())
                .connectTimeout(config.clientConnectTimeout())
                .concurrency(config.clientMaxConnections(), config.clientConnectionsPerRoute())
                .setDefaultHeader("Accept", "application/json")
                .followRedirects(false)
                .enableCookieManagement(false)
                .addShutdownHook(true);

        if (config.isProxied()) {
            clientInstanceConfig.proxy(
                    config.clientProxyHost(),
                    config.clientProxyPort(),
                    config.clientProxyUsername(),
                    config.clientProxyPassword()
            );
        }
    }

    @Override
    public Schema getSchemaById(int id) {
        logger.debug("Get schema by id: {}", id);

        HttpResponse<String> response = clientInstance.get(String.format("%s/%s/schemas/%s", SCHEMAKEEPER_URL, API_VERSION, id))
                .asString()
                .ifFailure(res -> {
                    logger.error("Error: {}. Status: {}", res.getBody(), res.getStatus());
                    throw new SchemaKeeperException(res.getBody());
                });

        try {
            SchemaMetadata schemaMetadata = mapper.readValue(response.getBody(), SchemaMetadata.class);
            logger.debug("Result of getting schema by id {}: {}", id, schemaMetadata);
            return schemaMetadata.getSchema();
        } catch (IOException e) {
            logger.error("Error while getting schema by id: {}. Error: {}", id, e.getLocalizedMessage());
            throw new SchemaKeeperException(e);
        }
    }

    @Override
    public int registerNewSchema(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
        logger.debug("Get schema id ({}) or register new schema and add to subject: {}", schema.toString(), subject);

           HttpResponse<String> response = clientInstance.post(String.format("%s/%s/subjects/%s/schemas", SCHEMAKEEPER_URL, API_VERSION, subject))
                .header("Content-Type", "application/json")
                .body(SubjectAndSchemaRequest.instance(schema, schemaType, compatibilityType))
                .asString()
                .ifFailure(res -> {
                    logger.error("Error: {}. Status: {}", res.getBody(), res.getStatus());
                    throw new SchemaKeeperException(res.getBody());
                });

        try {
            SchemaId schemaId = mapper.readValue(response.getBody(), SchemaId.class);
            logger.debug("Result of registering schema {} for subject {}: {}", schema.toString(), subject, schemaId);
            return schemaId.getSchemaId();
        } catch (IOException e) {
            logger.error("Error while registering new schema for subject: {} and schema: {}. Error: {}", subject, schema, e.getLocalizedMessage());
            throw new SchemaKeeperException(e);
        }
    }

    @Override
    public int getSchemaId(String subject, Schema schema, SchemaType schemaType) {
        logger.debug("Get schema id ({}) subject: {}", schema.toString(), subject);

        HttpResponse<String> response = clientInstance.post(String.format("%s/%s/subjects/%s/schemas/id", SCHEMAKEEPER_URL, API_VERSION, subject))
                .header("Content-Type", "application/json")
                .body(SchemaText.instance(schema, schemaType))
                .asString()
                .ifFailure(res -> {
                    logger.error("Error: {}. Status: {}", res.getBody(), res.getStatus());
                    throw new SchemaKeeperException(res.getBody());
                });

        try {
            SchemaId schemaId = mapper.readValue(response.getBody(), SchemaId.class);
            logger.debug("Result of getting schema id {} for subject {}: {}", schema.toString(), subject, schemaId);
            return schemaId.getSchemaId();
        } catch (IOException e) {
            logger.error("Error while getting schema id for subject: {} and schema: {}. Error: {}", subject, schema, e.getLocalizedMessage());
            throw new SchemaKeeperException(e);
        }
    }

    @Override
    public void close() {
        clientInstance.shutDown();
    }
}
