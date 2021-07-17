namespace java schemakeeper.generated.thrift

enum E {
  X = 1,
  Y = 2,
  Z = 3,
}

struct Nested {
  1: i32 x
}

union FooOrBar {
  1: string foo;
  2: string bar;
}


struct Test {
  1: bool boolField
  2: byte byteField
 16: optional byte byteOptionalField
  3: i16 i16Field
 15: optional i16 i16OptionalField
  4: optional i32 i32Field
  5: i64 i64Field
  6: double doubleField
  7: string stringField
  17: optional string stringOptionalFieldWithDefault = "default",
  8: optional binary binaryField
  9: map<string,i32> mapField
 10: list<i32> listField
 11: set<i32> setField
 12: E enumField
 13: Nested structField
 14: FooOrBar fooOrBar
}

struct ThriftMsgV1 {
  1: required string f1,
  2: optional string f2 = "test"
}

struct ThriftMsgV2 {
  1: required i64 f1
}

struct ThriftMsgV3 {
  3: optional string f3
}

struct ThriftMsgV4 {
  1: required string f1,
  3: required string f3,
  4: optional string f4
}

struct ThriftMsgV5 {
  1: required string f1,
  2: optional string f2 = "test",
  3: optional string f3,
  4: optional string f4
}
