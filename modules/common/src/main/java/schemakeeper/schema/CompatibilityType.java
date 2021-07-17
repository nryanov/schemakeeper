package schemakeeper.schema;

import java.util.HashMap;
import java.util.Map;

public enum CompatibilityType {
    NONE("none"),
    BACKWARD("backward"),
    FORWARD("forward"),
    FULL("full"),
    BACKWARD_TRANSITIVE("backward_transitive"),
    FORWARD_TRANSITIVE("forward_transitive"),
    FULL_TRANSITIVE("full_transitive");

    private static final Map<String, CompatibilityType> nameToCompatibilityTypeMap;

    static {
        nameToCompatibilityTypeMap = new HashMap<>();
        nameToCompatibilityTypeMap.put("none", NONE);
        nameToCompatibilityTypeMap.put("backward", BACKWARD);
        nameToCompatibilityTypeMap.put("forward", FORWARD);
        nameToCompatibilityTypeMap.put("full", FULL);
        nameToCompatibilityTypeMap.put("backward_transitive", BACKWARD_TRANSITIVE);
        nameToCompatibilityTypeMap.put("forward_transitive", FORWARD_TRANSITIVE);
        nameToCompatibilityTypeMap.put("full_transitive", FULL_TRANSITIVE);
    }

    public final String identifier;

    CompatibilityType(String identifier) {
        this.identifier = identifier;
    }

    public static CompatibilityType findByName(String name) {
        return nameToCompatibilityTypeMap.getOrDefault(name.toLowerCase(), BACKWARD);
    }
}
