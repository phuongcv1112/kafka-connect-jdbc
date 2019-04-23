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

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SystemTransactionCriteria {

  /**
   * The values that can be used in a statement's WHERE clause.
   */
  public interface CriteriaValues {

    /**
     * Get the last incremented value seen.
     *
     * @return the last incremented value from one of the rows
     * @throws SQLException if there is a problem accessing the value
     */
    Long lastIncrementedValue() throws SQLException;
  }

  protected static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final ColumnId systemTransactionColumn;
  protected final String convertedSystemTransactionColumnName;


  public SystemTransactionCriteria(
        ColumnId systemTransactionColumn
  ) {
      this.systemTransactionColumn = systemTransactionColumn;
      this.convertedSystemTransactionColumnName = new StringBuilder().append(this.systemTransactionColumn.name()).append("::text::bigint").toString();
  }

  /**
   * Build the WHERE clause for the columns used in this criteria.
   *
   * @param builder the string builder to which the WHERE clause should be appended; never null
   */
  public void whereClause(ExpressionBuilder builder) {
    systemTransactionWhereClause(builder);
  }

  /**
   * Set the query parameters on the prepared statement whose WHERE clause was generated with the
   * previous call to {@link #whereClause(ExpressionBuilder)}.
   *
   * @param stmt   the prepared statement; never null
   * @param values the values that can be used in the criteria parameters; never null
   * @throws SQLException if there is a problem using the prepared statement
   */
  public void setQueryParameters(
      PreparedStatement stmt,
      CriteriaValues values
  ) throws SQLException {
    setQueryParametersSystemTransaction(stmt, values);
  }

  protected void setQueryParametersSystemTransaction(
          PreparedStatement stmt,
          CriteriaValues values
  ) throws SQLException {
    Long incOffset = values.lastIncrementedValue();
    stmt.setLong(1, incOffset);
    log.debug("Executing prepared statement with incrementing value = {}", incOffset);
  }

  /**
   * Extract the offset values from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the timestamp for this row; may not be null
   */
  public SystemTransactionOffset extractValues(
      Schema schema,
      Struct record,
      SystemTransactionOffset previousOffset
  ) {
    Long extractedId = null;
    extractedId = extractOffsetSystemTransactionId(schema, record);

    // The system transaction column must be incrementing
    assert previousOffset == null || previousOffset.getIncrementingOffset() == -1L
            || extractedId > previousOffset.getIncrementingOffset();
    return new SystemTransactionOffset(extractedId);
  }

  /**
   * Extract the system transaction column value from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the incrementing ID for this row; may not be null
   */
  protected Long extractOffsetSystemTransactionId(
          Schema schema,
          Struct record
  ) {
    final Long extractedId;
    final Schema systemTransactionColumnSchema = schema.field(systemTransactionColumn.name()).schema();
    final Object systemTransactionColumnValue = record.get(systemTransactionColumn.name());
    if (systemTransactionColumnValue == null) {
      throw new ConnectException(
              "Null value for system transaction column of type: " + systemTransactionColumnSchema.type());
    } else if (isIntegralPrimitiveType(systemTransactionColumnValue)) {
      extractedId = ((Number) systemTransactionColumnValue).longValue();
    } else if (systemTransactionColumnSchema.name() != null && systemTransactionColumnSchema.name().equals(
            Decimal.LOGICAL_NAME)) {
      extractedId = extractDecimalId(systemTransactionColumnValue);
    } else {
      throw new ConnectException(
              "Invalid type for system transaction column: " + systemTransactionColumnSchema.type());
    }
    log.trace("Extracted system transaction column value: {}", extractedId);
    return extractedId;
  }

  protected Long extractDecimalId(Object incrementingColumnValue) {
    final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
    if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
      throw new ConnectException("Decimal value for system transaction column exceeded Long.MAX_VALUE");
    }
    if (decimal.scale() != 0) {
      throw new ConnectException("Scale of Decimal value for system transaction column must be 0");
    }
    return decimal.longValue();
  }

  protected boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long || incrementingColumnValue instanceof Integer
           || incrementingColumnValue instanceof Short || incrementingColumnValue instanceof Byte;
  }

  protected void systemTransactionWhereClause(ExpressionBuilder builder) {
    builder.append(" WHERE ");
    builder.append(convertedSystemTransactionColumnName);
    builder.append(" > ?");
    builder.append(" ORDER BY ");
    builder.append(systemTransactionColumn);
    builder.append(" ASC");
  }

}
