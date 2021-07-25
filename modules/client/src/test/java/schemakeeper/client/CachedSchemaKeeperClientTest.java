package schemakeeper.client;

import kong.unirest.Unirest;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import schemakeeper.schema.CompatibilityType;
import schemakeeper.schema.SchemaType;
import schemakeeper.serialization.SerDeConfig;

import java.util.Collections;
import static org.junit.Assert.*;

public class CachedSchemaKeeperClientTest {
    private static final Logger logger = LoggerFactory.getLogger(CachedSchemaKeeperClientTest.class);
    private static final GenericContainer schemakeeperServer = new GenericContainer("schemakeeper:test")
            .withExposedPorts(9081)
            .waitingFor(Wait.forHttp("/v2/subjects"))
            .withLogConsumer(new Slf4jLogConsumer(logger));
    private static SerDeConfig config;

    @BeforeClass
    public static void start() {
        schemakeeperServer.start();
        config = new SerDeConfig(Collections.singletonMap(SerDeConfig.SCHEMAKEEPER_URL_CONFIG, "http://localhost:" + schemakeeperServer.getMappedPort(9081)));
    }

    @AfterClass
    public static void stop() {
        schemakeeperServer.stop();
        Unirest.shutDown();
    }

    @After
    public void reset() {
        Unirest.config().reset();
    }

    @Test
    public void registerNewSchema() {
        CacheProxy client = new CacheProxy();

        assertTrue(client.getSubjectSchemas().isEmpty());
        int id  = client.registerNewSchema("A", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertTrue(client.getSubjectSchemas().containsKey("A"));
        assertEquals(id, client.getSubjectSchemas().get("A").get(Schema.create(Schema.Type.STRING)).intValue());

        int repeat = client.registerNewSchema("A", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertEquals(1, client.getSubjectSchemas().size());
        assertEquals(id, repeat);
        assertEquals(1, client.restCallsCount);
    }

    @Test
    public void registerMultipleSubjectsWithSimilarSchemas() {
        CacheProxy client = new CacheProxy();

        assertTrue(client.getSubjectSchemas().isEmpty());
        int id = client.registerNewSchema("A1", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertTrue(client.getSubjectSchemas().containsKey("A1"));
        assertEquals(id, client.getSubjectSchemas().get("A1").get(Schema.create(Schema.Type.STRING)).intValue());

        int repeat = client.registerNewSchema("B1", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertEquals(repeat, client.getSubjectSchemas().get("B1").get(Schema.create(Schema.Type.STRING)).intValue());
        assertEquals(2, client.getSubjectSchemas().size());
        assertEquals(id, repeat);
        assertEquals(2, client.restCallsCount);
    }

    @Test
    public void registerMultipleSubjectsWithDifferentSchemas() {
        CacheProxy client = new CacheProxy();

        assertTrue(client.getSubjectSchemas().isEmpty());
        int id  = client.registerNewSchema("A2", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertTrue(client.getSubjectSchemas().containsKey("A2"));
        assertEquals(id, client.getSubjectSchemas().get("A2").get(Schema.create(Schema.Type.STRING)).intValue());

        int repeat = client.registerNewSchema("B2", Schema.create(Schema.Type.INT), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertEquals(repeat, client.getSubjectSchemas().get("B2").get(Schema.create(Schema.Type.INT)).intValue());
        assertEquals(2, client.getSubjectSchemas().size());
        assertNotEquals(id, repeat);
        assertEquals(2, client.restCallsCount);
    }

    @Test
    public void registerSingleSubjectWithDifferentSchemas() {
        CacheProxy client = new CacheProxy();

        assertTrue(client.getSubjectSchemas().isEmpty());
        int id  = client.registerNewSchema("C", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertTrue(client.getSubjectSchemas().containsKey("C"));
        assertEquals(id, client.getSubjectSchemas().get("C").get(Schema.create(Schema.Type.STRING)).intValue());

        int repeat = client.registerNewSchema("C", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        assertEquals(id, client.getSubjectSchemas().get("C").get(Schema.create(Schema.Type.STRING)).intValue());
        assertEquals(id, repeat);
        assertEquals(1, client.restCallsCount);
    }

    @Test
    public void getSchemaById() {
        CachedSchemaKeeperClient client2 = new CachedSchemaKeeperClient(config);
        CacheProxy client = new CacheProxy();

        assertTrue(client.getIdToSchema().isEmpty());
        int id  = client2.registerNewSchema("D", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        client.getSchemaById(id);
        Schema schema = client.getSchemaById(id);

        assertEquals(1, client.getIdToSchema().size());
        assertEquals(Schema.create(Schema.Type.STRING), client.getIdToSchema().get(id));
        assertEquals(schema, Schema.create(Schema.Type.STRING));
        assertEquals(1, client.restCallsCount);
    }

    @Test
    public void getSchemaId() {
        CacheProxy client1 = new CacheProxy();
        CachedSchemaKeeperClient client2 = new CachedSchemaKeeperClient(config);

        assertTrue(client1.getSubjectSchemas().isEmpty());
        int id  = client2.registerNewSchema("E", Schema.create(Schema.Type.STRING), SchemaType.AVRO, CompatibilityType.BACKWARD);

        client1.getSchemaId("E", Schema.create(Schema.Type.STRING), SchemaType.AVRO);
        int result = client1.getSchemaId("E", Schema.create(Schema.Type.STRING), SchemaType.AVRO);

        assertEquals(id, client1.getSubjectSchemas().get("E").get(Schema.create(Schema.Type.STRING)).intValue());
        assertEquals(1, client1.getSubjectSchemas().size());
        assertEquals(result, id);
        assertEquals(1, client1.restCallsCount);

    }

    private class CacheProxy extends CachedSchemaKeeperClient {
        public int restCallsCount = 0;

        public CacheProxy() {
            super(CachedSchemaKeeperClientTest.config);
        }

        @Override
        public Schema getSchemaByIdRest(int id) {
            restCallsCount++;
            return super.getSchemaByIdRest(id);
        }

        @Override
        public int registerNewSchemaRest(String subject, Schema schema, SchemaType schemaType, CompatibilityType compatibilityType) {
            restCallsCount++;
            return super.registerNewSchemaRest(subject, schema, schemaType, compatibilityType);
        }

        @Override
        public int getSchemaIdRest(String subject, Schema schema, SchemaType schemaType) {
            restCallsCount++;
            return super.getSchemaIdRest(subject, schema, schemaType);
        }
    }
}
