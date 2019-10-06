package schemakeeper.client;

import kong.unirest.Unirest;
import org.apache.avro.Schema;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import schemakeeper.exception.SchemaKeeperException;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultSchemaKeeperClientTest {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchemaKeeperClientTest.class);
    private static final GenericContainer schemakeeperServer = new GenericContainer("schemakeeper-server:0.1")
            .withExposedPorts(9081)
            .waitingFor(Wait.forHttp("/v1/subjects"))
            .withLogConsumer(new Slf4jLogConsumer(logger));
    private static SerDeConfig config;

    @BeforeAll
    public static void start() {
        schemakeeperServer.start();
        config = new SerDeConfig(Collections.singletonMap(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "http://localhost:" + schemakeeperServer.getMappedPort(9081)));
    }

    @AfterAll
    public static void stop() {
        schemakeeperServer.stop();
        Unirest.shutDown();
    }

    @AfterEach
    public void reset() {
        Unirest.config().reset();
    }

    @Test
    public void getSchemaId() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        int id = client.registerNewSchema("A1", Schema.create(Schema.Type.INT), SchemaType.AVRO, CompatibilityType.BACKWARD);
        int result = client.getSchemaId("A1", Schema.create(Schema.Type.INT), SchemaType.AVRO);

        assertEquals(id, result);
    }


    @Test
    public void subjectDoesNotExist() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        client.registerNewSchema("A2", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);
        assertThrows(SchemaKeeperException.class, () -> client.getSchemaId("A123", Schema.create(Schema.Type.STRING), SchemaType.AVRO));
    }

    @Test
    public void incorrectSchemaIdError() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        assertThrows(SchemaKeeperException.class, () -> client.getSchemaById(-1));
    }

    @Test
    public void getSchemaById() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        int id = client.registerNewSchema("A3", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);
        Schema schema = client.getSchemaById(id);

        assertEquals(schema, Schema.create(Schema.Type.STRING));
    }

    @Test
    public void schemaWithSuchIdDoesNotExist() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        assertThrows(SchemaKeeperException.class, () -> client.getSchemaById(Integer.MAX_VALUE));
    }

    @Test
    public void subjectIsNotConnectedWithSchemaErrorWhileGettingSchemaId() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        client.registerNewSchema("A4", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);
        client.registerNewSchema("A5", Schema.create(Schema.Type.INT), SchemaType.AVRO, CompatibilityType.BACKWARD);
        assertThrows(SchemaKeeperException.class, () -> client.getSchemaId("A5", Schema.create(Schema.Type.STRING), SchemaType.AVRO));
    }

    @Test
    public void registerSchema() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        int id = client.registerNewSchema("A6", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);
        int result = client.getSchemaId("A6", Schema.create(Schema.Type.STRING), SchemaType.AVRO);

        assertEquals(id, result);
    }

    @Test
    public void schemaIsNotCompatibleErrorWhileRegisterNewSchema() {
        DefaultSchemaKeeperClient client = new DefaultSchemaKeeperClient(config);
        client.registerNewSchema("A7", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);
        assertThrows(SchemaKeeperException.class, () -> client.registerNewSchema("A7", Schema.create(Schema.Type.INT), SchemaType.AVRO, CompatibilityType.BACKWARD));
    }
}