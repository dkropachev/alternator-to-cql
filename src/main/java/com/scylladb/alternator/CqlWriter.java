package com.scylladb.alternator;

import com.datastax.oss.driver.api.core.CqlSession;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Writes DynamoDB items to Alternator tables via CQL with {@code USING TIMESTAMP} for
 * last-write-wins reconciliation.
 *
 * <p>Serializes DynamoDB attributes directly to Alternator's internal CQL representation using
 * {@link AlternatorSerializer}, then inserts into the target table via CQL with an explicit
 * microsecond timestamp. No staging table or DynamoDB roundtrip is needed.
 */
public class CqlWriter implements ItemWriter {

  private static final Logger LOG = Logger.getLogger(CqlWriter.class.getName());

  private final CqlSession cqlSession;
  private final String targetTable;
  private final String pkAttributeName;
  private final String timestampAttributeName;

  /**
   * Creates a new writer.
   *
   * @param cqlSession CQL session used to write to the target table
   * @param targetTable the Alternator table to write into
   * @param pkAttributeName the name of the partition key attribute in the DynamoDB item
   * @param timestampAttributeName the name of the attribute containing the wall-clock millisecond
   *     timestamp (a Number attribute); converted to microseconds for CQL
   */
  public CqlWriter(
      CqlSession cqlSession,
      String targetTable,
      String pkAttributeName,
      String timestampAttributeName) {
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
   *   <li>Serialize non-key attributes to Alternator's {@code :attrs} map format
   *   <li>Insert the partition key and {@code :attrs} into the target table via CQL with {@code
   *       USING TIMESTAMP}
   * </ol>
   *
   * @param item the DynamoDB item (must include the partition key and timestamp attributes)
   * @throws IllegalArgumentException if the timestamp is in the future or required attributes are
   *     missing
   */
  @Override
  public void write(Map<String, AttributeValue> item) {
    // 1. Validate required attributes
    AttributeValue pkValue = item.get(pkAttributeName);
    if (pkValue == null) {
      throw new IllegalArgumentException(
          "Item does not contain partition key attribute '" + pkAttributeName + "'");
    }
    Object cqlPkValue = toCqlPkValue(pkValue);

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

    // 2. Build the :attrs map from non-key attributes
    Map<ByteBuffer, ByteBuffer> attrsMap =
        AlternatorSerializer.serializeAttrsMap(item, pkAttributeName);

    // 3. Insert into target table via CQL
    String targetKs = "alternator_" + targetTable;
    long timestampMicros = lastUpdatedMillis * 1000L;

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (\"%s\", \":attrs\") VALUES (?, ?) USING TIMESTAMP ?",
            targetKs, targetTable, pkAttributeName);

    cqlSession.execute(cqlSession.prepare(cql).bind(cqlPkValue, attrsMap, timestampMicros));
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
