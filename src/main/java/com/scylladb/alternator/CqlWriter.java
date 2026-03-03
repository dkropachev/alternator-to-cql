package com.scylladb.alternator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Writes DynamoDB items to Alternator tables via CQL with {@code USING TIMESTAMP} for last-write-wins
 * reconciliation.
 *
 * <p>Alternator stores non-key attributes in an opaque {@code :attrs} blob map with no public
 * serializer. This class uses a "reference-write-then-CQL-clone" technique: it writes the item via
 * the DynamoDB API to a dedicated staging table, reads it back via CQL to obtain the raw
 * representation, then inserts it into the target table via CQL with an explicit microsecond
 * timestamp.
 *
 * <p><b>NOT thread-safe</b> for concurrent writes with the same partition key to the staging table.
 * Concurrent writes with different partition keys are safe.
 */
public class CqlWriter implements ItemWriter {

  private static final Logger LOG = Logger.getLogger(CqlWriter.class.getName());

  private final DynamoDbWriter stagingWriter;
  private final CqlSession cqlSession;
  private final String targetTable;
  private final String pkAttributeName;
  private final String timestampAttributeName;

  /**
   * Creates a new writer.
   *
   * @param dynamoClient DynamoDB client used to write reference rows to the staging table
   * @param cqlSession CQL session used to read from staging and write to target tables
   * @param stagingTableName pre-existing Alternator table with the same key schema as the target
   *     table (caller is responsible for creating it)
   * @param targetTable the Alternator table to write into
   * @param pkAttributeName the name of the partition key attribute in the DynamoDB item
   * @param timestampAttributeName the name of the attribute containing the wall-clock millisecond
   *     timestamp (a Number attribute); converted to microseconds for CQL
   */
  public CqlWriter(
      DynamoDbClient dynamoClient,
      CqlSession cqlSession,
      String stagingTableName,
      String targetTable,
      String pkAttributeName,
      String timestampAttributeName) {
    this(
        new DynamoDbWriter(dynamoClient, stagingTableName),
        cqlSession,
        targetTable,
        pkAttributeName,
        timestampAttributeName);
  }

  /**
   * Creates a new writer from pre-built components.
   *
   * @param stagingWriter writer for the DynamoDB staging table
   * @param cqlSession CQL session used to read from staging and write to target tables
   * @param targetTable the Alternator table to write into
   * @param pkAttributeName the name of the partition key attribute in the DynamoDB item
   * @param timestampAttributeName the name of the attribute containing the wall-clock millisecond
   *     timestamp (a Number attribute); converted to microseconds for CQL
   */
  public CqlWriter(
      DynamoDbWriter stagingWriter,
      CqlSession cqlSession,
      String targetTable,
      String pkAttributeName,
      String timestampAttributeName) {
    this.stagingWriter = stagingWriter;
    this.cqlSession = cqlSession;
    this.targetTable = targetTable;
    this.pkAttributeName = pkAttributeName;
    this.timestampAttributeName = timestampAttributeName;
  }

  /**
   * Writes an item to the target table via CQL with the timestamp extracted from the item.
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Extract and validate the timestamp from the item (must not be in the future)
   *   <li>Write the item to the staging table via the DynamoDB API
   *   <li>Read it back from the staging table via CQL
   *   <li>Insert all columns into the target table via CQL with {@code USING TIMESTAMP}
   * </ol>
   *
   * <p>Staging rows are not cleaned up after the write.
   *
   * @param item the DynamoDB item (must include the partition key and timestamp attributes)
   * @throws IllegalArgumentException if the timestamp is in the future or required attributes are
   *     missing
   */
  @Override
  public void write(Map<String, AttributeValue> item) {
    // 1. Extract and validate timestamp
    AttributeValue tsValue = item.get(timestampAttributeName);
    if (tsValue == null || tsValue.n() == null) {
      throw new IllegalArgumentException(
          "Item does not contain numeric timestamp attribute '" + timestampAttributeName + "'");
    }
    long lastUpdatedMillis = Long.parseLong(tsValue.n());

    if (lastUpdatedMillis > System.currentTimeMillis()) {
      String msg =
          "Rejecting timestamp "
              + lastUpdatedMillis
              + " — it is in the future (now="
              + System.currentTimeMillis()
              + ")";
      LOG.severe(msg);
      throw new IllegalArgumentException(msg);
    }

    // 2. Write to staging via DynamoDB API
    stagingWriter.write(item);

    // 3. Read from staging via CQL
    AttributeValue pkValue = item.get(pkAttributeName);
    if (pkValue == null) {
      throw new IllegalArgumentException(
          "Item does not contain partition key attribute '" + pkAttributeName + "'");
    }
    Object cqlPkValue = toCqlPkValue(pkValue);

    String stagingTableName = stagingWriter.getTableName();
    String stagingKs = "alternator_" + stagingTableName;
    Row stagingRow =
        cqlSession
            .execute(
                String.format(
                    "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\" = ?",
                    stagingKs, stagingTableName, pkAttributeName),
                cqlPkValue)
            .one();
    if (stagingRow == null) {
      throw new IllegalStateException(
          "Failed to read back staging row for pk=" + pkValue + " from table " + stagingTableName);
    }

    // 4. Build and execute CQL INSERT for target table
    String targetKs = "alternator_" + targetTable;
    long timestampMicros = lastUpdatedMillis * 1000L;

    List<String> colNames = new ArrayList<>();
    List<Object> colValues = new ArrayList<>();

    for (ColumnDefinition col : stagingRow.getColumnDefinitions()) {
      String name = col.getName().asCql(true);
      colNames.add(name);
      colValues.add(stagingRow.getObject(col.getName()));
    }

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) USING TIMESTAMP %d",
            targetKs,
            targetTable,
            String.join(", ", colNames),
            String.join(", ", Collections.nCopies(colNames.size(), "?")),
            timestampMicros);

    cqlSession.execute(cqlSession.prepare(cql).bind(colValues.toArray()));
  }

  private static Object toCqlPkValue(AttributeValue value) {
    if (value.s() != null) {
      return value.s();
    }
    if (value.n() != null) {
      return new java.math.BigDecimal(value.n());
    }
    if (value.b() != null) {
      return value.b().asByteBuffer();
    }
    throw new IllegalArgumentException("Unsupported partition key type: " + value);
  }
}
