package schemakeeper.schema;

public enum CompatibilityType {
    NONE,
    BACKWARD,
    FORWARD,
    FULL,
    BACKWARD_TRANSITIVE,
    FORWARD_TRANSITIVE,
    FULL_TRANSITIVE
}
