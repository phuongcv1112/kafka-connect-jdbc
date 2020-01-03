package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;
import org.mockito.Mock;
import java.sql.ResultSet;
import org.junit.runners.Parameterized.Parameter;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.StringJoiner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;


@RunWith(Parameterized.class)
public class PostgreSqlDatabaseDialectTypeTest extends BaseDialectTest<PostgreSqlDatabaseDialect> {
  protected SchemaBuilder schemaBuilder;
  protected PostgreSqlDatabaseDialect.ColumnConverter converter;

  protected static final TableId TABLE_ID = new TableId(null, null, "MyTable");
  protected static final ColumnId COLUMN_ID = new ColumnId(TABLE_ID, "columnA", "aliasA");

  protected static final String UUID_STRING = "c81d4e2e-bcf2-11e6-869b-7df92533d2db";
  protected static final UUID UUID_VALUE = UUID.fromString(UUID_STRING);
  protected static final String ARRAY_STRING = "{\"one two\",\"three four\"}";
  protected static final String[] ARRAY_VALUE = {"one two","three four"};

  @Mock
  ColumnDefinition columnDefn = mock(ColumnDefinition.class);

  @Mock
  ResultSet resultSet = mock(ResultSet.class);

  @Override
  protected PostgreSqlDatabaseDialect createDialect() {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
  }

  protected String dumpArrayToString(String[] stringArray) {
    StringJoiner joiner = new StringJoiner("\",\"", "{\"", "\"}");
    for (String item: stringArray) {
      joiner.add(item);
    }
    return joiner.toString();
  }

  @Parameterized.Parameter(0)
  public Schema.Type expectedType;

  @Parameterized.Parameter(1)
  public Object expectedValue;

  @Parameterized.Parameter(2)
  public int columnType;

  @Parameterized.Parameter(3)
  public String columnTypeName;

  @Parameterized.Parameter(4)
  public Object inputValue;

  @Parameterized.Parameters
  public static Iterable<Object[]> mapping() {
    return Arrays.asList(
        new Object[][] {
            {Schema.Type.STRING, UUID_STRING, Types.OTHER, "uuid", UUID_VALUE},
            {Schema.Type.STRING, ARRAY_STRING, Types.ARRAY, "ARRAY", ARRAY_VALUE},
        }
    );
  }

  @Test
  public void testValueConversionOnUuid() throws Exception {
    when(columnDefn.type()).thenReturn(columnType);
    when(columnDefn.typeName()).thenReturn(columnTypeName);
    when(columnDefn.id()).thenReturn(COLUMN_ID);

    dialect = createDialect();
    schemaBuilder = SchemaBuilder.struct();

    dialect.addFieldToSchema(columnDefn, schemaBuilder);
    Schema schema = schemaBuilder.build();
    List<Field> fields = schema.fields();
    assertEquals(1, fields.size());
    Field field = fields.get(0);
    assertEquals(expectedType, field.schema().type());

    if (inputValue instanceof String[]) {
      when(resultSet.getString(1)).thenReturn(dumpArrayToString((String[]) inputValue));
    } else {
      when(resultSet.getString(1)).thenReturn(inputValue.toString());
    }

    ColumnMapping mapping = new ColumnMapping(columnDefn, 1, field);
    converter = dialect.createColumnConverter(
      mapping
    );
    Object value = converter.convert(resultSet);
    assertEquals(expectedValue, value);
  }
}
