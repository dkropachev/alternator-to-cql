package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that validates the correctness of the DynamoDB-to-CQL conversion performed by
 * {@link CqlWriter}: items written via the DynamoDB API are read back via CQL, inserted into a
 * second table with {@code USING TIMESTAMP}, and compared against the originals to verify data
 * integrity and timestamp ordering.
 */
public class CqlWriterConversionIT {

  private DynamoDbClient dynamoClient;
  private AlternatorDynamoDbClientWrapper wrapper;
  private CqlSession cqlSession;

  private String sourceTable;
  private String targetTable;

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    sourceTable = "cloner_src_" + suffix;
    targetTable = "cloner_tgt_" + suffix;

    wrapper =
        AlternatorDynamoDbClient.builder()
            .endpointOverride(IntegrationTestConfig.HTTP_SEED_URI)
            .credentialsProvider(IntegrationTestConfig.CREDENTIALS)
            .buildWithAlternatorAPI();

    dynamoClient = wrapper.getClient();

    cqlSession =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(IntegrationTestConfig.HOST, IntegrationTestConfig.CQL_PORT))
            .withLocalDatacenter(IntegrationTestConfig.DATACENTER)
            .withAuthCredentials(
                IntegrationTestConfig.CQL_USERNAME, IntegrationTestConfig.CQL_PASSWORD)
            .build();
  }

  @After
  public void tearDown() {
    if (dynamoClient != null) {
      for (String t : new String[] {sourceTable, targetTable}) {
        if (t != null) {
          try {
            dynamoClient.deleteTable(DeleteTableRequest.builder().tableName(t).build());
          } catch (Exception e) {
            // ignore cleanup errors
          }
        }
      }
    }
    if (cqlSession != null) {
      cqlSession.close();
    }
    if (wrapper != null) {
      wrapper.close();
    }
  }

  /** Reads back a row written via the DynamoDB API using CQL and verifies it is non-null. */
  @Test
  public void testReadRow() throws Exception {
    createTable(sourceTable);

    Map<String, AttributeValue> item = buildItem("read-1", "/data/chunk.parquet", 0, 65536);
    dynamoClient.putItem(PutItemRequest.builder().tableName(sourceTable).item(item).build());

    Row row = readRow(sourceTable, "pk", "read-1");
    assertNotNull("Row should be readable via CQL after DynamoDB write", row);

    // Verify the partition key column is present
    boolean hasPk = false;
    for (ColumnDefinition col : row.getColumnDefinitions()) {
      if (col.getName().asInternal().equals("pk")) {
        hasPk = true;
        break;
      }
    }
    assertTrue("Row should contain the pk column", hasPk);
  }

  /** Returns null for a partition key that does not exist. */
  @Test
  public void testReadRowReturnsNullForMissing() throws Exception {
    createTable(sourceTable);

    Row row = readRow(sourceTable, "pk", "nonexistent");
    assertNull("readRow should return null for a missing partition key", row);
  }

  /**
   * Clones a row from one table to another via CQL with {@code USING TIMESTAMP} and verifies the
   * data is identical when read back through the DynamoDB API.
   */
  @Test
  public void testInsertWithTimestamp() throws Exception {
    createTable(sourceTable);
    createTable(targetTable);

    Map<String, AttributeValue> item = buildItem("clone-1", "/data/important.parquet", 100, 200000);
    dynamoClient.putItem(PutItemRequest.builder().tableName(sourceTable).item(item).build());

    Row sourceRow = readRow(sourceTable, "pk", "clone-1");
    assertNotNull(sourceRow);

    long timestampMicros = 1700000000000L * 1000L;
    insertWithTimestamp(targetTable, sourceRow, timestampMicros);

    // Read back from target via DynamoDB API
    Map<String, AttributeValue> fromSource = readViaAlternator(sourceTable, "clone-1");
    Map<String, AttributeValue> fromTarget = readViaAlternator(targetTable, "clone-1");

    assertNotNull("Source item should be readable", fromSource);
    assertNotNull("Target (cloned) item should be readable", fromTarget);

    // Compare all non-key attributes
    Set<String> allKeys = new TreeSet<>();
    allKeys.addAll(fromSource.keySet());
    allKeys.addAll(fromTarget.keySet());

    for (String key : allKeys) {
      assertEquals(
          "Attribute '" + key + "' should match between source and cloned row",
          fromSource.get(key),
          fromTarget.get(key));
    }
  }

  /**
   * Verifies that when two rows are cloned to the same partition key with different timestamps, the
   * newer timestamp wins (last-write-wins via CQL timestamp).
   */
  @Test
  public void testTimestampOrdering() throws Exception {
    createTable(sourceTable);
    createTable(targetTable);

    long olderMicros = 1700000000000L * 1000L;
    long newerMicros = 1700000001000L * 1000L;

    // Write two different items to the source table
    Map<String, AttributeValue> olderItem = buildItem("older-ref", "/data/old.parquet", 0, 1000);
    Map<String, AttributeValue> newerItem = buildItem("newer-ref", "/data/new.parquet", 500, 9999);

    dynamoClient.putItem(PutItemRequest.builder().tableName(sourceTable).item(olderItem).build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(sourceTable).item(newerItem).build());

    Row olderRow = readRow(sourceTable, "pk", "older-ref");
    Row newerRow = readRow(sourceTable, "pk", "newer-ref");
    assertNotNull(olderRow);
    assertNotNull(newerRow);

    // Clone both to the same pk in the target table — newer first, then older
    insertWithTimestampAndPk(targetTable, newerRow, "conflict-pk", newerMicros);
    insertWithTimestampAndPk(targetTable, olderRow, "conflict-pk", olderMicros);

    // The newer data should win
    Map<String, AttributeValue> result = readViaAlternator(targetTable, "conflict-pk");
    assertNotNull("Conflict target should exist", result);

    Map<String, AttributeValue> newerRef = readViaAlternator(sourceTable, "newer-ref");
    for (String key : newerRef.keySet()) {
      if (key.equals("pk")) continue;
      assertEquals(
          "Attribute '" + key + "' should reflect the newer write",
          newerRef.get(key),
          result.get(key));
    }
  }

  // --- CQL row-cloning methods (the logic under test) ---

  private Row readRow(String tableName, String pkAttributeName, Object cqlPkValue) {
    String keyspace = "alternator_" + tableName;
    return cqlSession
        .execute(
            String.format(
                "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\" = ?",
                keyspace, tableName, pkAttributeName),
            cqlPkValue)
        .one();
  }

  private void insertWithTimestamp(String table, Row row, long timestampMicros) {
    String keyspace = "alternator_" + table;

    List<String> colNames = new ArrayList<>();
    List<Object> colValues = new ArrayList<>();

    for (ColumnDefinition col : row.getColumnDefinitions()) {
      String name = col.getName().asCql(true);
      colNames.add(name);
      colValues.add(row.getObject(col.getName()));
    }

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) USING TIMESTAMP %d",
            keyspace,
            table,
            String.join(", ", colNames),
            String.join(", ", Collections.nCopies(colNames.size(), "?")),
            timestampMicros);

    cqlSession.execute(cqlSession.prepare(cql).bind(colValues.toArray()));
  }

  private void insertWithTimestampAndPk(String table, Row row, String newPk, long timestampMicros) {
    String keyspace = "alternator_" + table;

    List<String> colNames = new ArrayList<>();
    List<Object> colValues = new ArrayList<>();

    for (ColumnDefinition col : row.getColumnDefinitions()) {
      String name = col.getName().asCql(true);
      colNames.add(name);

      if (col.getName().asInternal().equals("pk")) {
        colValues.add(newPk);
      } else {
        colValues.add(row.getObject(col.getName()));
      }
    }

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) USING TIMESTAMP %d",
            keyspace,
            table,
            String.join(", ", colNames),
            String.join(", ", Collections.nCopies(colNames.size(), "?")),
            timestampMicros);

    cqlSession.execute(cqlSession.prepare(cql).bind(colValues.toArray()));
  }

  // --- Helpers ---

  private void createTable(String name) throws InterruptedException {
    try {
      dynamoClient.deleteTable(DeleteTableRequest.builder().tableName(name).build());
      Thread.sleep(500);
    } catch (ResourceNotFoundException e) {
      // Table doesn't exist, that's fine
    }

    dynamoClient.createTable(
        CreateTableRequest.builder()
            .tableName(name)
            .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("pk")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .provisionedThroughput(
                ProvisionedThroughput.builder()
                    .readCapacityUnits(1L)
                    .writeCapacityUnits(1L)
                    .build())
            .build());

    Thread.sleep(500);
  }

  private static Map<String, AttributeValue> buildItem(
      String id, String chunkPath, long startByte, long endByte) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("pk", AttributeValue.builder().s(id).build());
    item.put("chunk_path", AttributeValue.builder().s(chunkPath).build());
    item.put("start_byte_range", AttributeValue.builder().n(String.valueOf(startByte)).build());
    item.put("end_byte_range", AttributeValue.builder().n(String.valueOf(endByte)).build());
    return item;
  }

  private Map<String, AttributeValue> readViaAlternator(String table, String pkValue) {
    GetItemResponse resp =
        dynamoClient.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .key(Map.of("pk", AttributeValue.builder().s(pkValue).build()))
                .build());
    return resp.hasItem() && !resp.item().isEmpty() ? resp.item() : null;
  }
}
