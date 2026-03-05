package com.scylladb.alternator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Logger;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Writes DynamoDB items to a pure CQL table with explicit typed columns using {@code USING
 * TIMESTAMP} for last-write-wins reconciliation.
 *
 * <p>Unlike {@link CqlWriter} which writes to Alternator's internal {@code :attrs} blob map, this
 * writer inserts into standard CQL tables where each DynamoDB attribute maps to a named, typed CQL
 * column.
 */
public class PureCqlWriter implements ItemWriter, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(PureCqlWriter.class.getName());

  /** Supported CQL column types for DynamoDB attribute mapping. */
  public enum CqlType {
    TEXT("text"),
    BIGINT("bigint"),
    INT("int"),
    BOOLEAN("boolean"),
    BLOB("blob"),
    DECIMAL("decimal"),
    DOUBLE("double"),
    FLOAT("float");

    private final String cqlName;

    CqlType(String cqlName) {
      this.cqlName = cqlName;
    }

    public String cqlName() {
      return cqlName;
    }
  }

  private final CqlSession cqlSession;
  private final boolean ownsCqlSession;
  private final String keyspace;
  private final String table;
  private final String pkAttributeName;
  private final String timestampAttributeName;
  private final LinkedHashMap<String, CqlType> columns;
  private PreparedStatement preparedInsert;

  /**
   * Creates a new writer using an externally managed CQL session.
   *
   * @param cqlSession CQL session used to write to the target table
   * @param keyspace the CQL keyspace containing the target table
   * @param table the CQL table name
   * @param pkAttributeName the name of the partition key attribute in the DynamoDB item
   * @param timestampAttributeName the name of the attribute containing the wall-clock millisecond
   *     timestamp (a Number attribute); converted to microseconds for CQL
   * @param columns ordered mapping of DynamoDB attribute names to CQL column types; must include
   *     the partition key column
   */
  public PureCqlWriter(
      CqlSession cqlSession,
      String keyspace,
      String table,
      String pkAttributeName,
      String timestampAttributeName,
      LinkedHashMap<String, CqlType> columns) {
    this.cqlSession = cqlSession;
    this.ownsCqlSession = false;
    this.keyspace = keyspace;
    this.table = table;
    this.pkAttributeName = pkAttributeName;
    this.timestampAttributeName = timestampAttributeName;
    this.columns = columns;
  }

  /**
   * Creates a new writer that manages its own CQL session.
   *
   * @param host the CQL contact point host
   * @param port the CQL native transport port
   * @param datacenter the local datacenter name
   * @param username the CQL username for authentication (may be {@code null} to skip auth)
   * @param password the CQL password for authentication (may be {@code null} to skip auth)
   * @param keyspace the CQL keyspace containing the target table
   * @param table the CQL table name
   * @param pkAttributeName the name of the partition key attribute in the DynamoDB item
   * @param timestampAttributeName the name of the attribute containing the wall-clock millisecond
   *     timestamp (a Number attribute); converted to microseconds for CQL
   * @param columns ordered mapping of DynamoDB attribute names to CQL column types; must include
   *     the partition key column
   */
  public PureCqlWriter(
      String host,
      int port,
      String datacenter,
      String username,
      String password,
      String keyspace,
      String table,
      String pkAttributeName,
      String timestampAttributeName,
      LinkedHashMap<String, CqlType> columns) {
    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress(host, port))
            .withLocalDatacenter(datacenter);
    if (username != null && password != null) {
      builder.withAuthCredentials(username, password);
    }
    this.cqlSession = builder.build();
    this.ownsCqlSession = true;
    this.keyspace = keyspace;
    this.table = table;
    this.pkAttributeName = pkAttributeName;
    this.timestampAttributeName = timestampAttributeName;
    this.columns = columns;
  }

  @Override
  public void write(Map<String, AttributeValue> item) {
    AttributeValue pkValue = item.get(pkAttributeName);
    if (pkValue == null) {
      throw new IllegalArgumentException(
          "Item does not contain partition key attribute '" + pkAttributeName + "'");
    }

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

    long timestampMicros = lastUpdatedMillis * 1000L;

    if (preparedInsert == null) {
      preparedInsert = prepareInsert();
    }

    List<Object> values = new ArrayList<>();
    for (Map.Entry<String, CqlType> col : columns.entrySet()) {
      AttributeValue av = item.get(col.getKey());
      values.add(convertValue(av, col.getValue(), col.getKey()));
    }
    values.add(timestampMicros);

    cqlSession.execute(preparedInsert.bind(values.toArray()));
  }

  @Override
  public void close() {
    if (ownsCqlSession && cqlSession != null) {
      cqlSession.close();
    }
  }

  private PreparedStatement prepareInsert() {
    StringJoiner colNames = new StringJoiner(", ");
    StringJoiner placeholders = new StringJoiner(", ");
    for (String name : columns.keySet()) {
      colNames.add(String.format("\"%s\"", name));
      placeholders.add("?");
    }
    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) USING TIMESTAMP ?",
            keyspace, table, colNames, placeholders);
    return cqlSession.prepare(cql);
  }

  private static Object convertValue(AttributeValue value, CqlType type, String attrName) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case TEXT:
        return value.s();
      case BIGINT:
        return Long.parseLong(value.n());
      case INT:
        return Integer.parseInt(value.n());
      case BOOLEAN:
        return value.bool();
      case BLOB:
        return value.b().asByteBuffer();
      case DECIMAL:
        return new BigDecimal(value.n());
      case DOUBLE:
        return Double.parseDouble(value.n());
      case FLOAT:
        return Float.parseFloat(value.n());
      default:
        throw new IllegalArgumentException(
            "Unsupported CQL type " + type + " for attribute '" + attrName + "'");
    }
  }
}
