/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package schemakeeper.generated.thrift;


import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.*;

import java.util.*;

// No additional import required for struct/union.

public class ThriftMsgV3 implements TBase<ThriftMsgV3, ThriftMsgV3._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("ThriftMsgV3");

  private static final TField F3_FIELD_DESC = new TField("f3", TType.STRING, (short)3);


  public String f3;

  /** The set of fields this object contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    F3((short)3, "f3");
  
    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();
  
    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }
  
    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 3: // F3
          return F3;
        default:
          return null;
      }
    }
  
    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }
  
    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }
  
    private final short _thriftId;
    private final String _fieldName;
  
    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }
  
    public short getThriftFieldId() {
      return _thriftId;
    }
  
    public String getFieldName() {
      return _fieldName;
    }
  }


  // isset id assignments

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  
  /**
   * FieldValueMetaData.type returns TType.STRING for both string and binary field values.
   * This set can be used to determine if a FieldValueMetaData with type TType.STRING is actually
   * declared as binary in the idl file.
   */
  public static final Set<FieldValueMetaData> binaryFieldValueMetaDatas;
  
  private static FieldValueMetaData registerBinaryFieldValueMetaData(FieldValueMetaData f, Set<FieldValueMetaData> binaryFieldValues) {
    binaryFieldValues.add(f);
    return f;
  }
  
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    Set<FieldValueMetaData> tmpSet = new HashSet<FieldValueMetaData>();
    tmpMap.put(_Fields.F3, new FieldMetaData("f3", TFieldRequirementType.OPTIONAL,
      new FieldValueMetaData(TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    binaryFieldValueMetaDatas = Collections.unmodifiableSet(tmpSet);
    FieldMetaData.addStructMetaDataMap(ThriftMsgV3.class, metaDataMap);
  }

  /**
   * Returns a map of the annotations and their values for this struct declaration.
   * See fieldAnnotations or valueAnnotations for the annotations attached to struct fields
   * or enum values.
   */
  public static final Map<String, String> structAnnotations;
  static {
    structAnnotations = Collections.emptyMap();
  }

  /**
   * Returns a map of the annotations for each of this struct's fields, keyed by the field.
   * See structAnnotations for the annotations attached to this struct's declaration.
   */
  public static final Map<_Fields, Map<String, String>> fieldAnnotations;
  static {
    fieldAnnotations = Collections.emptyMap();
  }

  /**
   * Returns the set of fields that have a configured default value.
   * The default values for these fields can be obtained by
   * instantiating this class with the default constructor.
   */
  public static final Set<_Fields> hasDefaultValue;
  static {
    Set<_Fields> tmp = EnumSet.noneOf(_Fields.class);
    hasDefaultValue = Collections.unmodifiableSet(tmp);
  }


  public ThriftMsgV3() {
  }


  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThriftMsgV3(ThriftMsgV3 other) {
    if (other.isSetF3()) {
      this.f3 = other.f3;
    }
  }

  public static List<String> validateNewInstance(ThriftMsgV3 item) {
    final List<String> buf = new ArrayList<String>();

    return buf;
  }

  public ThriftMsgV3 deepCopy() {
    return new ThriftMsgV3(this);
  }

  @Override
  public void clear() {
    this.f3 = null;
  }

  public String getF3() {
    return this.f3;
  }

  public ThriftMsgV3 setF3(String f3) {
    this.f3 = f3;
    
    return this;
  }

  public void unsetF3() {
    this.f3 = null;
  }

  /** Returns true if field f3 is set (has been assigned a value) and false otherwise */
  public boolean isSetF3() {
    return this.f3 != null;
  }

  public void setF3IsSet(boolean value) {
    if (!value) {
      this.f3 = null;
    }
  }

  @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case F3:
      if (value == null) {
        unsetF3();
      } else {
        setF3((String)value);
      }
      break;
    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case F3:
      return getF3();
    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case F3:
      return isSetF3();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ThriftMsgV3)
      return this.equals((ThriftMsgV3)that);
    return false;
  }

  public boolean equals(ThriftMsgV3 that) {
    return equalsWithoutPassthrough(that);
  }

  private boolean equalsWithoutPassthrough(ThriftMsgV3 that) {
    if (that == null)
      return false;
    boolean this_present_f3 = true && this.isSetF3();
    boolean that_present_f3 = true && that.isSetF3();
    if (this_present_f3 || that_present_f3) {
      if (!(this_present_f3 && that_present_f3))
        return false;
      if (!this.f3.equals(that.f3))
        return false;
    }
    return true;
  }


  @Override
  public int hashCode() {
    int hashCode = 1;
    if (isSetF3()) {
      hashCode = 31 * hashCode + f3.hashCode();
    }
    return hashCode;
  }

  public int compareTo(ThriftMsgV3 other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ThriftMsgV3 typedOther = (ThriftMsgV3)other;

    lastComparison = Boolean.valueOf(isSetF3()).compareTo(typedOther.isSetF3());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetF3()) {
      lastComparison = TBaseHelper.compareTo(this.f3, typedOther.f3);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      switch (field.id) {
        case 3: // F3
          this.f3 = iprot.readString();
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();
    
    oprot.writeStructBegin(STRUCT_DESC);
    if (this.f3 != null) {
      if (isSetF3()) {
        oprot.writeFieldBegin(F3_FIELD_DESC);
        oprot.writeString(this.f3);
        oprot.writeFieldEnd();
      }
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftMsgV3(");
    boolean first = true;
    if (isSetF3()) {
      sb.append("f3:");
      if (this.f3 == null) {
        sb.append("null");
      } else {
        sb.append(this.f3);
      }
      first = false;
      }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }
}

