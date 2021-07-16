package schemakeeper.client.protocol;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import schemakeeper.api.SchemaText;

import java.io.IOException;

public class SchemaTextSerializer extends StdSerializer<SchemaText> {
    public SchemaTextSerializer() {
        this(null);
    }

    public SchemaTextSerializer(Class<SchemaText> t) {
        super(t);
    }

    @Override
    public void serialize(SchemaText value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("schemaText", value.getSchemaText());
        gen.writeStringField("schemaType", value.getSchemaType().identifier);
        gen.writeEndObject();
    }
}
