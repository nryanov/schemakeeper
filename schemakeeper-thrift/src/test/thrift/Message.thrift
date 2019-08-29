namespace java schemakeeper.serialization.thrift.test

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
