package schemakeeper.schema;

import java.util.HashMap;
import java.util.Map;

public enum SchemaType {
    AVRO("avro"),
    THRIFT("thrift"),
    PROTOBUF("protobuf");

    private static final Map<String, SchemaType> nameToSchemaTypeMap;

    static {
        nameToSchemaTypeMap = new HashMap<>();
        nameToSchemaTypeMap.put("avro", AVRO);
        nameToSchemaTypeMap.put("thrift", THRIFT);
        nameToSchemaTypeMap.put("protobuf", PROTOBUF);
    }

    public final String identifier;

    SchemaType(String identifier) {
        this.identifier = identifier;
    }

    public static SchemaType findByName(String name) {
        return nameToSchemaTypeMap.getOrDefault(name.toLowerCase(), AVRO);
    }
}
