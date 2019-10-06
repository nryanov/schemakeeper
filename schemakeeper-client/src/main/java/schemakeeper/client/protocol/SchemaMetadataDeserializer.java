package schemakeeper.client.protocol;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import schemakeeper.api.SchemaMetadata;
import schemakeeper.schema.SchemaType;

import java.io.IOException;

public class SchemaMetadataDeserializer extends StdDeserializer<SchemaMetadata> {
    public SchemaMetadataDeserializer() {
        this(null);
    }

    public SchemaMetadataDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SchemaMetadata deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        int schemaId = node.get("schemaId").asInt();
        String schemaText = node.get("schemaText").asText();
        String schemaHash = node.get("schemaHash").asText();
        String schemaType = node.get("schemaType").asText();

        return SchemaMetadata.instance(schemaId, schemaText, schemaHash, SchemaType.findByName(schemaType));
    }
}
