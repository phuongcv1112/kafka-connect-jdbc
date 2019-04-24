/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class SystemTransactionCriteriaTest {
  // Since system transaction column is a incrementing value like id  (after convert, not monothlic)
  // below test cases are almost like testcases of incrementing mode
  private static final TableId TABLE_ID = new TableId(null, null,"myTable");
  private static final ColumnId INCREMENTING_COLUMN = new ColumnId(TABLE_ID, "id");

  private IdentifierRules rules;
  private QuoteMethod identifierQuoting;
  private ExpressionBuilder builder;
  private SystemTransactionCriteria criteriaInc;
  private Schema schema;
  private Struct record;

  @Before
  public void beforeEach() {
    criteriaInc = new SystemTransactionCriteria(INCREMENTING_COLUMN);
    identifierQuoting = null;
    rules = null;
    builder = null;
  }

  protected void assertExtractedOffset(long expected, Schema schema, Struct record) {
    SystemTransactionCriteria criteria = null;
    if (schema.field(INCREMENTING_COLUMN.name()) != null) {
      criteria = criteriaInc;
    }
    SystemTransactionOffset offset = criteria.extractValues(schema, record, null);
    assertEquals(expected, offset.getIncrementingOffset());
  }

  @Test
  public void extractIntOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractLongOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    record = new Struct(schema).put("id", 42L);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(42));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal("42.42"));
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractWithIncColumn() throws SQLException {
    schema = SchemaBuilder.struct()
                          .field("id", SchemaBuilder.INT32_SCHEMA)
                          .build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);

  }

  @Test
  public void createIncrementingWhereClause() {
    builder = builder();
    criteriaInc.systemTransactionWhereClause(builder);
    assertEquals(
        " WHERE id::text::bigint > ? ORDER BY id::text::bigint ASC",
        builder.toString()
    );

  }

  protected ExpressionBuilder builder() {
    ExpressionBuilder result = new ExpressionBuilder(rules);
    result.setQuoteIdentifiers(identifierQuoting);
    return result;
  }
}