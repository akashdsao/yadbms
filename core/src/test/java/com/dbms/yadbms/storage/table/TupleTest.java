package com.dbms.yadbms.storage.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dbms.yadbms.catalog.Column;
import com.dbms.yadbms.catalog.Schema;
import com.dbms.yadbms.type.TypeId;
import com.dbms.yadbms.type.Value;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TupleTest {

  @Test
  void testTinyInt() {
    Schema schema = new Schema(List.of(new Column("col", TypeId.TINYINT)));
    Value v = new Value(TypeId.TINYINT, (byte) 123);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals((byte) 123, t.getValue(schema, 0).asTinyInt());

//    Tuple t2 = serAndDeSer(t);
//    assertEquals((byte) 123, t2.getValue(schema, 0).asTinyInt());
  }

  @Test
  void testSmallInt() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.SMALLINT)));
    Value v = new Value(TypeId.SMALLINT, (short) 12345);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals((short) 12345, t.getValue(schema, 0).asSmallInt());

    Tuple t2 = serAndDeSer(t);
    assertEquals((short) 12345, t2.getValue(schema, 0).asSmallInt());
  }

  @Test
  void testInteger() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.INTEGER)));
    Value v = new Value(TypeId.INTEGER, 123456);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals(123456, t.getValue(schema, 0).asInt());

    Tuple t2 = serAndDeSer(t);
    assertEquals(123456, t2.getValue(schema, 0).asInt());
  }

  @Test
  void testBigInt() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.BIGINT)));
    Value v = new Value(TypeId.BIGINT, 1234567890123L);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals(1234567890123L, t.getValue(schema, 0).asBigInt());

    Tuple t2 = serAndDeSer(t);
    assertEquals(1234567890123L, t2.getValue(schema, 0).asBigInt());
  }

  @Test
  void testDecimal() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.DECIMAL)));
    Value v = new Value(TypeId.DECIMAL, 123.456);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals(123.456, t.getValue(schema, 0).asDecimal());

    Tuple t2 = serAndDeSer(t);
    assertEquals(123.456, t2.getValue(schema, 0).asDecimal());
  }

  @Test
  void testBoolean() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.BOOLEAN)));
    Value v = new Value(TypeId.BOOLEAN, true);

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals(true, t.getValue(schema, 0).asBoolean());

    Tuple t2 = serAndDeSer(t);
    assertEquals(true, t2.getValue(schema, 0).asBoolean());
  }

  @Test
  void testVarchar() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.VARCHAR, 100)));
    Value v = new Value(TypeId.VARCHAR, "Alice");

    Tuple t = new Tuple(List.of(v), schema);
    assertEquals("Alice", t.getValue(schema, 0).asString());

    Tuple t2 = serAndDeSer(t);
    assertEquals("Alice", t2.getValue(schema, 0).asString());
  }

  @Test
  void testVector() {
    Schema schema = new Schema(List.of(new Column("c1", TypeId.VECTOR, 3)));
    Value v = new Value(TypeId.VECTOR, new double[] {1.1, 2.2, 3.3});

    Tuple t = new Tuple(List.of(v), schema);
    assertArrayEquals(new double[] {1.1, 2.2, 3.3}, t.getValue(schema, 0).asVector(), 1e-9);

    Tuple t2 = serAndDeSer(t);
    assertArrayEquals(new double[] {1.1, 2.2, 3.3}, t2.getValue(schema, 0).asVector(), 1e-9);
  }

  @Test
  void testComplexTupleBuildAndGetValue() {
    List<Column> columns =
        Arrays.asList(
            new Column("id", TypeId.INTEGER),
            new Column("name", TypeId.VARCHAR, 50),
            new Column("balance", TypeId.BIGINT));
    Schema schema = new Schema(columns);
    List<Value> values =
        Arrays.asList(
            new Value(TypeId.INTEGER, 101),
            new Value(TypeId.VARCHAR, "Alice"),
            new Value(TypeId.BIGINT, 5000L));

    Tuple tuple = new Tuple(values, schema);
    assertEquals(101, tuple.getValue(schema, 0).asInt());
    assertEquals("Alice", tuple.getValue(schema, 1).asString());
    assertEquals(5000L, tuple.getValue(schema, 2).asBigInt());
  }

  private Tuple serAndDeSer(Tuple t) {
    byte[] storage = new byte[1024];
    t.serializeTo(storage, 0);
    Tuple t2 = new Tuple();
    t2.deserializeFrom(storage, 0);
    return t2;
  }
}
