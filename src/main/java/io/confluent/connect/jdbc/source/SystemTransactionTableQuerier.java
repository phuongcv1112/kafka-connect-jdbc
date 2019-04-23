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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.confluent.connect.jdbc.source.SystemTransactionCriteria.CriteriaValues;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;


/**
 * <p>
 *   SystemTransactionTableQuerier performs incremental loading of data using main mechanisms:
 *   a system transaction column provided monotonically incrementing values that can be used to
 *   detect new or modified rows.
 * </p>
 *
 * <p>
 *     Currently, this table querier only apply for postgreSQL with system column xmin. Detail
 *     of this column can be found in https://www.postgresql.org/docs/9.1/ddl-system-columns.html
 * </p>
 *
 */

public class SystemTransactionTableQuerier extends TableQuerier implements CriteriaValues {
  private static final Logger log = LoggerFactory.getLogger(
      SystemTransactionTableQuerier.class
  );

//  private final List<String> timestampColumnNames;
//  private final List<ColumnId> timestampColumns;
//  private String incrementingColumnName;
//  private long timestampDelay;
  private String systemTransactionColumnName;
  private SystemTransactionOffset offset;
  private SystemTransactionCriteria criteria;
  private final Map<String, String> partition;
  private final String topic;
//  private final TimeZone timeZone;

  public SystemTransactionTableQuerier(DatabaseDialect dialect, QueryMode mode, String name,
                                       String topicPrefix,
                                       String systemTransactionColumnName,
//                                       List<String> timestampColumnNames,
//                                       String incrementingColumnName,
                                       Map<String, Object> offsetMap
                                       //, Long timestampDelay
//                                       TimeZone timeZone
                                      ) {
    super(dialect, mode, name, topicPrefix);
//    this.incrementingColumnName = incrementingColumnName;
//    this.timestampColumnNames = timestampColumnNames != null
//                                ? timestampColumnNames : Collections.<String>emptyList();
//    this.timestampDelay = timestampDelay;
    this.systemTransactionColumnName = systemTransactionColumnName;
    this.offset = SystemTransactionOffset.fromMap(offsetMap);

//    this.timestampColumns = new ArrayList<>();
//    for (String timestampColumn : this.timestampColumnNames) {
//      if (timestampColumn != null && !timestampColumn.isEmpty()) {
//        timestampColumns.add(new ColumnId(tableId, timestampColumn));
//      }
//    }

    switch (mode) {
      case TABLE:
        String tableName = tableId.tableName();
        topic = topicPrefix + tableName;// backward compatible
        partition = OffsetProtocols.sourcePartitionForProtocolV1(tableId);
        break;
//      case QUERY:
//        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
//            JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
//        topic = topicPrefix;
//        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }

//    this.timeZone = timeZone;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
//    findDefaultAutoIncrementingColumn(db);

//    ColumnId incrementingColumn = null;
//    if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
//      incrementingColumn = new ColumnId(tableId, incrementingColumnName);
//    }
    ColumnId systemTransactionColumn = new ColumnId(tableId, systemTransactionColumnName);

    ExpressionBuilder builder = dialect.expressionBuilder();
    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ");
        builder.append(tableId);
        break;
//      case QUERY:
//        builder.append(query);
//        break;
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
    }

    // Append the criteria using the columns ...
    criteria = dialect.criteriaFor(systemTransactionColumn);
    criteria.whereClause(builder);

    String queryString = builder.toString();
    recordQuery(queryString);
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = dialect.createPreparedStatement(db, queryString);
  }

//  private void findDefaultAutoIncrementingColumn(Connection db) throws SQLException {
//    // Default when unspecified uses an autoincrementing column
//    if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
//      // Find the first auto-incremented column ...
//      for (ColumnDefinition defn : dialect.describeColumns(
//          db,
//          tableId.catalogName(),
//          tableId.schemaName(),
//          tableId.tableName(),
//          null).values()) {
//        if (defn.isAutoIncrement()) {
//          incrementingColumnName = defn.id().name();
//          break;
//        }
//      }
//    }
//    // If still not found, query the table and use the result set metadata.
//    // This doesn't work if the table is empty.
//    if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
//      log.debug("Falling back to describe '{}' table by querying {}", tableId, db);
//      for (ColumnDefinition defn : dialect.describeColumnsByQuerying(db, tableId).values()) {
//        if (defn.isAutoIncrement()) {
//          incrementingColumnName = defn.id().name();
//          break;
//        }
//      }
//    }
//  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    criteria.setQueryParameters(stmt, this);
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = new Struct(schemaMapping.schema());
    for (FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Ignoring record because processing failed:", e);
      } catch (SQLException e) {
        log.warn("Ignoring record due to SQL error:", e);
      }
    }
    offset = criteria.extractValues(schemaMapping.schema(), record, offset);
    return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
  }

//  @Override
//  public Timestamp beginTimetampValue() {
//    return offset.getTimestampOffset();
//  }

//  @Override
//  public Timestamp endTimetampValue()  throws SQLException {
//    final long currentDbTime = dialect.currentTimeOnDB(
//        stmt.getConnection(),
//        DateTimeUtils.getTimeZoneCalendar(timeZone)
//    ).getTime();
//    return new Timestamp(currentDbTime - timestampDelay);
//  }

  @Override
  public Long lastIncrementedValue() {
    return offset.getIncrementingOffset();
  }

  @Override
  public String toString() {
    return "SystemTransactionTableQuerier{"
           + "table=" + tableId
           + ", query='" + query + '\''
           + ", topicPrefix='" + topicPrefix
           + '}';
  }
}
