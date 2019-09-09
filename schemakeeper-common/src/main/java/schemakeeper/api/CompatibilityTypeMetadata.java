package schemakeeper.api;

import schemakeeper.schema.CompatibilityType;

import java.util.Objects;

public class CompatibilityTypeMetadata {
    private CompatibilityType compatibilityType;

    public static CompatibilityTypeMetadata instance(CompatibilityType compatibilityType) {
        return new CompatibilityTypeMetadata(compatibilityType);
    }

    public CompatibilityTypeMetadata() {
        this.compatibilityType = CompatibilityType.BACKWARD;
    }

    public CompatibilityTypeMetadata(CompatibilityType compatibilityType) {
        this.compatibilityType = compatibilityType;
    }

    public CompatibilityType getCompatibilityType() {
        return compatibilityType;
    }

    public void setCompatibilityType(CompatibilityType compatibilityType) {
        this.compatibilityType = compatibilityType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompatibilityTypeMetadata that = (CompatibilityTypeMetadata) o;
        return compatibilityType == that.compatibilityType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(compatibilityType);
    }

    @Override
    public String toString() {
        return "CompatibilityTypeMetadata{" +
                "compatibilityType=" + compatibilityType +
                '}';
    }
}
