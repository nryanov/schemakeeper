package schemakeeper.schema.thrift;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.thrift.ThriftData;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.TType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;

/**
 * See: https://issues.apache.org/jira/projects/AVRO/issues/AVRO-2539
 */
public class SchemaKeeperThriftData extends ThriftData {
    static final String THRIFT_TYPE = "thrift";
    static final String THRIFT_PROP = "thrift";

    private static final SchemaKeeperThriftData INSTANCE = new SchemaKeeperThriftData();

    protected SchemaKeeperThriftData() {
    }

    /**
     * Return the singleton instance.
     */
    public static SchemaKeeperThriftData get() {
        return INSTANCE;
    }

    private final Map<Class, Schema> schemaCache = new ConcurrentHashMap<>();

    /**
     * Return a record schema given a thrift generated class.
     */
    @SuppressWarnings("unchecked")
    public Schema getSchema(Class c) {
        Schema schema = schemaCache.get(c);

        if (schema == null) { // cache miss
            try {
                if (TEnum.class.isAssignableFrom(c)) { // enum
                    List<String> symbols = new ArrayList<>();
                    for (Enum e : ((Class<? extends Enum>) c).getEnumConstants())
                        symbols.add(e.name());
                    schema = Schema.createEnum(c.getName(), null, null, symbols);
                } else if (TBase.class.isAssignableFrom(c)) { // struct
                    TBase instance = ((Class<? extends TBase>) c).getConstructor().newInstance();
                    schema = Schema.createRecord(c.getName(), null, null, Throwable.class.isAssignableFrom(c));
                    List<Schema.Field> fields = new ArrayList<>();

                    FieldMetaData.getStructMetaDataMap((Class<? extends TBase>) c).forEach((key, value) -> {
                        Schema s;
                        Object defaultValue = null;
                        if (value.requirementType == TFieldRequirementType.OPTIONAL) {
                            defaultValue = getOptionalDefaultValue(instance, key);
                            s = getSchema(value.valueMetaData, defaultValue != NULL_DEFAULT_VALUE);
                            if ((s.getType() != Schema.Type.UNION)) {
                                s = nullable(s, defaultValue != NULL_DEFAULT_VALUE);
                            }
                        } else {
                            s = getSchema(value.valueMetaData);
                        }

                        fields.add(new Schema.Field(value.fieldName, s, null, defaultValue));
                    });

                    schema.setFields(fields);
                } else {
                    throw new RuntimeException("Not a Thrift-generated class: " + c);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            schemaCache.put(c, schema); // update cache
        }
        return schema;
    }

    private static final Schema NULL = Schema.create(Schema.Type.NULL);

    private Schema getSchema(FieldValueMetaData f) {
        return getSchema(f, false);
    }

    private Schema getSchema(FieldValueMetaData f, boolean hasNotNullDefaultValue) {
        switch (f.type) {
            case TType.BOOL:
                return Schema.create(Schema.Type.BOOLEAN);
            case TType.BYTE:
                Schema b = Schema.create(Schema.Type.INT);
                b.addProp(THRIFT_PROP, "byte");
                return b;
            case TType.I16:
                Schema s = Schema.create(Schema.Type.INT);
                s.addProp(THRIFT_PROP, "short");
                return s;
            case TType.I32:
                return Schema.create(Schema.Type.INT);
            case TType.I64:
                return Schema.create(Schema.Type.LONG);
            case TType.DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case TType.ENUM:
                EnumMetaData enumMeta = (EnumMetaData) f;
                return nullable(getSchema(enumMeta.enumClass), hasNotNullDefaultValue);
            case TType.LIST:
                ListMetaData listMeta = (ListMetaData) f;
                return nullable(Schema.createArray(getSchema(listMeta.elemMetaData)), hasNotNullDefaultValue);
            case TType.MAP:
                MapMetaData mapMeta = (MapMetaData) f;
                if (mapMeta.keyMetaData.type != TType.STRING)
                    throw new AvroRuntimeException("Map keys must be strings: " + f);
                Schema map = Schema.createMap(getSchema(mapMeta.valueMetaData));
                GenericData.setStringType(map, GenericData.StringType.String);
                return nullable(map, hasNotNullDefaultValue);
            case TType.SET:
                SetMetaData setMeta = (SetMetaData) f;
                Schema set = Schema.createArray(getSchema(setMeta.elemMetaData));
                set.addProp(THRIFT_PROP, "set");
                return nullable(set, hasNotNullDefaultValue);
            case TType.STRING:
                if (f.isBinary())
                    return nullable(Schema.create(Schema.Type.BYTES), hasNotNullDefaultValue);
                Schema string = Schema.create(Schema.Type.STRING);
                GenericData.setStringType(string, GenericData.StringType.String);
                return nullable(string, hasNotNullDefaultValue);
            case TType.STRUCT:
                StructMetaData structMeta = (StructMetaData) f;
                Schema record = getSchema(structMeta.structClass);
                return nullable(record, hasNotNullDefaultValue);
            case TType.VOID:
                return NULL;
            default:
                throw new RuntimeException("Unexpected type in field: " + f);
        }
    }

    @SuppressWarnings("unchecked")
    private Object getOptionalDefaultValue(TBase instance, TFieldIdEnum field) {
        if (instance.isSet(field)) {
            return null;
        } else {
            return NULL_DEFAULT_VALUE;
        }
    }

    private Schema nullable(Schema schema) {
        return nullable(schema, false);
    }

    /**
     * Avro recommends to set default value type as first type in union schema.
     * So, if the current schema has some not null default type, then this type should be first,
     * otherwise the NULL type should be first.
     */
    private Schema nullable(Schema schema, boolean hasNotNullDefaultValue) {
        if (hasNotNullDefaultValue) {
            return Schema.createUnion(Arrays.asList(schema, NULL));
        }

        return Schema.createUnion(Arrays.asList(NULL, schema));
    }

}
