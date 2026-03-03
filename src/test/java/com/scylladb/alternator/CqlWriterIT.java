package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that writes data via the Alternator (DynamoDB) API to one table, then writes the
 * same data via CQL with {@code USING TIMESTAMP} to a second table, and verifies the CQL-written
 * data is correctly readable via Alternator with no differences.
 *
 * <p>Uses the "clone-from-reference" technique: write reference rows via Alternator to table A,
 * read their raw CQL representation (including the opaque {@code :attrs} blob map), then insert
 * copies into table B via CQL with an explicit microsecond timestamp.
 */
public class CqlWriterIT {

  private static final Logger LOG = Logger.getLogger(CqlWriterIT.class.getName());

  private DynamoDbClient dynamoClient;
  private AlternatorDynamoDbClientWrapper wrapper;
  private CqlSession cqlSession;

  /** Table A — written via Alternator API (the reference/source). */
  private String tableA;

  /** Table B — written via CQL with USING TIMESTAMP (the target). */
  private String tableB;

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    tableA = "cqlts_a_" + suffix;
    tableB = "cqlts_b_" + suffix;

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
            .build();
  }

  @After
  public void tearDown() {
    if (dynamoClient != null) {
      for (String t : new String[] {tableA, tableB}) {
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

  /** Baseline: write a CatalogRecord-shaped item via Alternator and read it back. */
  @Test
  public void testBaselineAlternatorWriteAndRead() throws Exception {
    createTable(tableA);

    Map<String, AttributeValue> item =
        buildCatalogItem("rec-1", "/data/chunk_001.parquet", 0, 65536, 1700000000000L, 2, 86400);

    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item).build());

    Map<String, AttributeValue> result = readViaAlternator(tableA, "rec-1");
    assertNotNull("Item should be readable via Alternator", result);
    assertFalse("Item should not be empty", result.isEmpty());

    assertEquals("/data/chunk_001.parquet", result.get("chunk_path").s());
    assertEquals("0", result.get("start_byte_range").n());
    assertEquals("65536", result.get("end_byte_range").n());
    assertEquals("1700000000000", result.get("last_updated_millis").n());
    assertEquals("2", result.get("compression_version").n());
    assertEquals("86400", result.get("ttl").n());
  }

  /**
   * Core validation:
   *
   * <ol>
   *   <li>Write items via Alternator API to table A
   *   <li>Create table B via Alternator API (so it has the same Alternator schema)
   *   <li>Read each item from table A via CQL, write it to table B via CQL with {@code USING
   *       TIMESTAMP}
   *   <li>Read table B via Alternator API
   *   <li>Assert no differences between table A and table B reads
   * </ol>
   */
  @Test
  public void testCqlWriteWithTimestampReadViaAlternator() throws Exception {
    createTable(tableA);
    createTable(tableB);

    // Write multiple items to table A via Alternator
    long ts1 = 1700000000000L;
    long ts2 = 1700000001000L;
    Map<String, AttributeValue> item1 =
        buildCatalogItem("rec-1", "/data/chunk_ref.parquet", 100, 200000, ts1, 3, 43200);
    Map<String, AttributeValue> item2 =
        buildCatalogItem("rec-2", "/data/chunk_two.parquet", 0, 50000, ts2, 1, 7200);

    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item1).build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item2).build());

    // For each item: read from table A via CQL, write to table B via CQL with USING TIMESTAMP
    for (String pk : List.of("rec-1", "rec-2")) {
      Row cqlRow = readCqlRow(tableA, pk);
      assertNotNull("Row " + pk + " should be readable via CQL from table A", cqlRow);

      Map<String, AttributeValue> altRow = readViaAlternator(tableA, pk);
      long lastUpdatedMillis = Long.parseLong(altRow.get("last_updated_millis").n());
      long timestampMicros = lastUpdatedMillis * 1000L;

      insertViaCqlWithTimestamp(tableB, cqlRow, pk, timestampMicros);
    }

    // Read both items from table B via Alternator and compare with table A
    for (String pk : List.of("rec-1", "rec-2")) {
      Map<String, AttributeValue> fromA = readViaAlternator(tableA, pk);
      Map<String, AttributeValue> fromB = readViaAlternator(tableB, pk);

      assertNotNull("Item " + pk + " should exist in table A", fromA);
      assertNotNull("Item " + pk + " should exist in table B (CQL-written)", fromB);

      // Compare all non-key attributes between tables
      Set<String> allKeys = new TreeSet<>();
      allKeys.addAll(fromA.keySet());
      allKeys.addAll(fromB.keySet());

      for (String key : allKeys) {
        if (key.equals("pk")) continue;
        assertEquals(
            "Attribute '"
                + key
                + "' for pk="
                + pk
                + " should match between Alternator-written (table A) and CQL-written (table B)",
            fromA.get(key),
            fromB.get(key));
      }
    }

    // Verify actual values from CQL-written table B against original input
    Map<String, AttributeValue> rec1 = readViaAlternator(tableB, "rec-1");
    assertEquals("/data/chunk_ref.parquet", rec1.get("chunk_path").s());
    assertEquals("100", rec1.get("start_byte_range").n());
    assertEquals("200000", rec1.get("end_byte_range").n());
    assertEquals("1700000000000", rec1.get("last_updated_millis").n());
    assertEquals("3", rec1.get("compression_version").n());
    assertEquals("43200", rec1.get("ttl").n());

    Map<String, AttributeValue> rec2 = readViaAlternator(tableB, "rec-2");
    assertEquals("/data/chunk_two.parquet", rec2.get("chunk_path").s());
    assertEquals("0", rec2.get("start_byte_range").n());
    assertEquals("50000", rec2.get("end_byte_range").n());
    assertEquals("1700000001000", rec2.get("last_updated_millis").n());
    assertEquals("1", rec2.get("compression_version").n());
    assertEquals("7200", rec2.get("ttl").n());
  }

  /**
   * Write two reference rows with different data to table A via Alternator, then clone them to the
   * same pk in table B via CQL — newer timestamp first, older timestamp second. The newer data
   * should win for all attributes.
   */
  @Test
  public void testNewerTimestampWins() throws Exception {
    createTable(tableA);
    createTable(tableB);

    long olderMillis = 1700000000000L;
    long newerMillis = 1700000001000L;

    // Write two reference rows with different data to table A
    Map<String, AttributeValue> olderItem =
        buildCatalogItem("ref-older", "/data/old_chunk.parquet", 0, 1000, olderMillis, 1, 3600);
    Map<String, AttributeValue> newerItem =
        buildCatalogItem("ref-newer", "/data/new_chunk.parquet", 500, 9999, newerMillis, 5, 172800);

    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(olderItem).build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(newerItem).build());

    // Read both via CQL from table A
    Row olderRow = readCqlRow(tableA, "ref-older");
    Row newerRow = readCqlRow(tableA, "ref-newer");
    assertNotNull("Older reference should be readable via CQL", olderRow);
    assertNotNull("Newer reference should be readable via CQL", newerRow);

    // Clone NEWER data first, then OLDER data to the same pk in table B
    String targetPk = "conflict-target";
    insertViaCqlWithTimestamp(tableB, newerRow, targetPk, newerMillis * 1000L);
    insertViaCqlWithTimestamp(tableB, olderRow, targetPk, olderMillis * 1000L);

    // Read from table B via Alternator — newer data should win
    Map<String, AttributeValue> result = readViaAlternator(tableB, targetPk);
    assertNotNull("Target row should exist in table B", result);

    // The newer reference values (from table A) should be present
    Map<String, AttributeValue> newerRef = readViaAlternator(tableA, "ref-newer");
    assertNotNull(newerRef);
    for (String key : newerRef.keySet()) {
      if (key.equals("pk")) continue;
      assertEquals(
          "Attribute '" + key + "' should reflect the newer write (newer timestamp wins)",
          newerRef.get(key),
          result.get(key));
    }

    // Verify actual values match the newer input
    assertEquals("/data/new_chunk.parquet", result.get("chunk_path").s());
    assertEquals("500", result.get("start_byte_range").n());
    assertEquals("9999", result.get("end_byte_range").n());
    assertEquals("1700000001000", result.get("last_updated_millis").n());
    assertEquals("5", result.get("compression_version").n());
    assertEquals("172800", result.get("ttl").n());
  }

  /**
   * Exercises the {@link CqlWriter} class: write items to table A via the writer (which uses a
   * staging table internally), then read them back via Alternator and compare with direct-written
   * reference data.
   */
  @Test
  public void testWriterClass() throws Exception {
    createTable(tableA);
    createTable(tableB);

    CqlWriter writer = new CqlWriter(cqlSession, tableB, "pk", "last_updated_millis");

    long ts1 = 1700000000000L;
    long ts2 = 1700000001000L;
    Map<String, AttributeValue> item1 =
        buildCatalogItem("wr-1", "/data/writer_one.parquet", 10, 20000, ts1, 2, 7200);
    Map<String, AttributeValue> item2 =
        buildCatalogItem("wr-2", "/data/writer_two.parquet", 0, 50000, ts2, 4, 86400);

    // Write items to table B via the writer
    writer.write(item1);
    writer.write(item2);

    // Also write to table A directly via Alternator for comparison
    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item1).build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item2).build());

    // Compare table A (Alternator-written) with table B (CqlWriter-written)
    for (String pk : List.of("wr-1", "wr-2")) {
      Map<String, AttributeValue> fromA = readViaAlternator(tableA, pk);
      Map<String, AttributeValue> fromB = readViaAlternator(tableB, pk);

      assertNotNull("Item " + pk + " should exist in table A", fromA);
      assertNotNull("Item " + pk + " should exist in table B (writer-written)", fromB);

      Set<String> allKeys = new TreeSet<>();
      allKeys.addAll(fromA.keySet());
      allKeys.addAll(fromB.keySet());

      for (String key : allKeys) {
        if (key.equals("pk")) continue;
        assertEquals(
            "Attribute '"
                + key
                + "' for pk="
                + pk
                + " should match between Alternator-written and CqlWriter-written",
            fromA.get(key),
            fromB.get(key));
      }
    }

    // Verify actual values from CqlWriter-written table B against original input
    Map<String, AttributeValue> wr1 = readViaAlternator(tableB, "wr-1");
    assertNotNull(wr1);
    assertEquals("/data/writer_one.parquet", wr1.get("chunk_path").s());
    assertEquals("10", wr1.get("start_byte_range").n());
    assertEquals("20000", wr1.get("end_byte_range").n());
    assertEquals("1700000000000", wr1.get("last_updated_millis").n());
    assertEquals("2", wr1.get("compression_version").n());
    assertEquals("7200", wr1.get("ttl").n());

    Map<String, AttributeValue> wr2 = readViaAlternator(tableB, "wr-2");
    assertNotNull(wr2);
    assertEquals("/data/writer_two.parquet", wr2.get("chunk_path").s());
    assertEquals("0", wr2.get("start_byte_range").n());
    assertEquals("50000", wr2.get("end_byte_range").n());
    assertEquals("1700000001000", wr2.get("last_updated_millis").n());
    assertEquals("4", wr2.get("compression_version").n());
    assertEquals("86400", wr2.get("ttl").n());
  }

  /**
   * Validates that {@link CqlWriter#write} throws {@link IllegalArgumentException} for future
   * timestamps.
   */
  @Test
  public void testWriterRejectsFutureTimestamp() throws Exception {
    createTable(tableA);
    createTable(tableB);

    CqlWriter writer = new CqlWriter(cqlSession, tableB, "pk", "last_updated_millis");

    long futureMillis = System.currentTimeMillis() + 3_600_000;
    Map<String, AttributeValue> item =
        buildCatalogItem("wr-future", "/data/f.parquet", 0, 100, futureMillis, 1, 3600);

    try {
      writer.write(item);
      fail("Expected IllegalArgumentException for future timestamp");
    } catch (IllegalArgumentException e) {
      assertTrue("Exception message should mention 'future'", e.getMessage().contains("future"));
    }

    // Verify nothing was written to table B
    Map<String, AttributeValue> result = readViaAlternator(tableB, "wr-future");
    assertNull("Future-timestamped row should not have been written", result);
  }

  /** Validate that future timestamps are rejected by the guard and no CQL write occurs. */
  @Test
  public void testRejectFutureTimestamp() throws Exception {
    createTable(tableA);
    createTable(tableB);

    long futureMillis = System.currentTimeMillis() + 3_600_000; // 1 hour in the future
    assertFalse("Future timestamp should be rejected", validateTimestamp(futureMillis));

    // Write a reference row to table A so we have something to clone
    Map<String, AttributeValue> item =
        buildCatalogItem("ref-future", "/data/f.parquet", 0, 100, futureMillis, 1, 3600);
    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item).build());

    Row refRow = readCqlRow(tableA, "ref-future");
    assertNotNull(refRow);

    // The guard prevents the CQL write to table B
    String targetPk = "should-not-exist";
    if (!validateTimestamp(futureMillis)) {
      LOG.severe(
          "Skipping CQL write for pk='"
              + targetPk
              + "': lastUpdatedMillis="
              + futureMillis
              + " is in the future");
    } else {
      insertViaCqlWithTimestamp(tableB, refRow, targetPk, futureMillis * 1000L);
    }

    // Target pk should not exist in table B
    Map<String, AttributeValue> result = readViaAlternator(tableB, targetPk);
    assertNull("Future-timestamped row should not have been written to table B", result);
  }

  /**
   * Writes 10,000 records with randomized data (including edge cases) and validates every single
   * one via the Alternator (DynamoDB) API.
   *
   * <p>For each unique key, multiple versions are written with different {@code
   * last_updated_millis} values — some with timestamps older than the current version ("looking at
   * the past") and some with timestamps newer ("looking at the future"). Regardless of write order,
   * the final result read via Alternator must always reflect the version with the highest {@code
   * last_updated_millis}.
   *
   * <p>Strategy: each version gets a unique staging pk in table A (so all versions coexist), then
   * all versions are cloned to table B under the real target pk via CQL with {@code USING
   * TIMESTAMP}. The writes are shuffled so older and newer timestamps arrive in random order.
   *
   * <p>Uses a fixed seed for reproducibility.
   */
  @Test
  public void testBulkWriteWithTimestampOrdering() throws Exception {
    createTable(tableA);
    createTable(tableB);

    Random rng = new Random(42);
    int totalRecords = 10_000;
    int uniqueKeys = 2_000;
    // 2000 unique keys × 5 versions each = 10,000 writes

    // Pre-generate edge-case chunk paths to mix in
    String[] edgePaths = {
      "",
      "/",
      "/data/a.parquet",
      "s3://bucket/prefix/chunk_0001.parquet",
      "/data/path with spaces/file.parquet",
      "/data/\u00e9l\u00e8ve/\u4e16\u754c.parquet",
      "/data/\ud83d\ude00\ud83d\udd25.parquet",
      "/data/quote\"slash\\colon:brace{}.parquet",
      "/data/newline\npath.parquet",
      "/data/tab\there.parquet",
      "/data/null\u0000byte.parquet",
      String.join("", Collections.nCopies(200, "x")) + ".parquet",
      "/data/UPPER/CASE/PATH.PARQUET",
      "/data/../../../etc/passwd",
      "/data/file\twith\ttabs.parquet",
    };

    // Base timestamp — far enough in the past that all variations stay in the past
    long baseTimestamp = System.currentTimeMillis() - 86_400_000L; // 24 hours ago

    // For each unique key, track the version with the highest last_updated_millis
    Map<String, Map<String, AttributeValue>> expectedItems = new HashMap<>();

    // Each write: staging pk (unique), target pk, timestamp, and item data
    // stagingPk is unique per version so all coexist in table A
    List<String[]> writeSpecs = new ArrayList<>(totalRecords); // [stagingPk, targetPk]
    List<Map<String, AttributeValue>> stagingItems = new ArrayList<>(totalRecords);

    for (int k = 0; k < uniqueKeys; k++) {
      String targetPk = String.format("user-%05d", k);

      // Generate 5 versions with different timestamps
      long[] timestamps = new long[5];
      for (int v = 0; v < 5; v++) {
        // Spread timestamps across a 1-hour window, with guaranteed gaps
        timestamps[v] = baseTimestamp - 3_600_000L + (v * 720_000L) + rng.nextInt(1000);
      }

      long maxTs = Long.MIN_VALUE;

      for (int v = 0; v < 5; v++) {
        String stagingPk = targetPk + "_v" + v;
        String chunkPath;
        long startByte;
        long endByte;
        int compressionVersion;
        int ttl;

        if (k < edgePaths.length) {
          chunkPath = edgePaths[k] + "_v" + v;
        } else {
          chunkPath = "/data/chunk_" + targetPk + "_v" + v + ".parquet";
        }

        // Mix in edge-case numeric values
        switch (v) {
          case 0:
            startByte = 0;
            endByte = 0;
            compressionVersion = 0;
            ttl = 0;
            break;
          case 1:
            startByte = Long.MAX_VALUE - rng.nextInt(1000);
            endByte = Long.MAX_VALUE;
            compressionVersion = Integer.MAX_VALUE;
            ttl = Integer.MAX_VALUE;
            break;
          case 2:
            startByte = rng.nextLong() & Long.MAX_VALUE; // positive random
            endByte = startByte + rng.nextInt(1_000_000);
            compressionVersion = rng.nextInt(100);
            ttl = rng.nextInt(604_800); // up to 7 days
            break;
          case 3:
            startByte = 1;
            endByte = 1;
            compressionVersion = 1;
            ttl = 1;
            break;
          default:
            startByte = rng.nextInt(1_000_000);
            endByte = startByte + rng.nextInt(10_000_000);
            compressionVersion = rng.nextInt(10);
            ttl = 3600 + rng.nextInt(82800);
            break;
        }

        // Staging item uses unique stagingPk so all versions coexist in table A
        Map<String, AttributeValue> stagingItem =
            buildCatalogItem(
                stagingPk, chunkPath, startByte, endByte, timestamps[v], compressionVersion, ttl);
        stagingItems.add(stagingItem);
        writeSpecs.add(new String[] {stagingPk, targetPk});

        // Track expected: the version with the highest timestamp (using targetPk)
        if (timestamps[v] > maxTs) {
          maxTs = timestamps[v];
          expectedItems.put(
              targetPk,
              buildCatalogItem(
                  targetPk, chunkPath, startByte, endByte, timestamps[v], compressionVersion, ttl));
        }
      }
    }

    // Phase 1: Write all 10K staging rows to table A via Alternator
    LOG.info("Writing " + totalRecords + " staging rows to table A...");
    for (int i = 0; i < stagingItems.size(); i++) {
      dynamoClient.putItem(
          PutItemRequest.builder().tableName(tableA).item(stagingItems.get(i)).build());
      if ((i + 1) % 2000 == 0) {
        LOG.info("  Staged " + (i + 1) + "/" + totalRecords);
      }
    }
    LOG.info("All staging rows written");

    // Phase 2: Shuffle the write order, then clone each staging row to table B via CQL
    // with USING TIMESTAMP — so older/newer timestamps arrive in random order
    List<Integer> indices = new ArrayList<>(totalRecords);
    for (int i = 0; i < totalRecords; i++) {
      indices.add(i);
    }
    Collections.shuffle(indices, rng);

    LOG.info("Cloning " + totalRecords + " rows to table B via CQL with USING TIMESTAMP...");
    int cloned = 0;
    for (int idx : indices) {
      String stagingPk = writeSpecs.get(idx)[0];
      String targetPk = writeSpecs.get(idx)[1];
      long lastUpdatedMillis = Long.parseLong(stagingItems.get(idx).get("last_updated_millis").n());
      long timestampMicros = lastUpdatedMillis * 1000L;

      Row cqlRow = readCqlRow(tableA, stagingPk);
      assertNotNull("Staging row " + stagingPk + " should be readable via CQL", cqlRow);

      insertViaCqlWithTimestamp(tableB, cqlRow, targetPk, timestampMicros);

      cloned++;
      if (cloned % 2000 == 0) {
        LOG.info("  Cloned " + cloned + "/" + totalRecords);
      }
    }
    LOG.info("All rows cloned");

    // Phase 3: Validate every unique key via Alternator API
    LOG.info("Validating " + uniqueKeys + " keys via Alternator API...");
    int validated = 0;
    int failures = 0;
    StringBuilder failureLog = new StringBuilder();

    for (int k = 0; k < uniqueKeys; k++) {
      String pk = String.format("user-%05d", k);

      Map<String, AttributeValue> expected = expectedItems.get(pk);
      assertNotNull("Expected item should exist for " + pk, expected);

      Map<String, AttributeValue> actual = readViaAlternator(tableB, pk);
      assertNotNull("Item " + pk + " should exist in table B", actual);

      // Validate every field against the expected (highest-timestamp) version
      for (String attr :
          List.of(
              "chunk_path",
              "start_byte_range",
              "end_byte_range",
              "last_updated_millis",
              "compression_version",
              "ttl")) {
        AttributeValue expectedVal = expected.get(attr);
        AttributeValue actualVal = actual.get(attr);

        if (!Objects.equals(expectedVal, actualVal)) {
          failures++;
          failureLog
              .append("MISMATCH pk=")
              .append(pk)
              .append(" attr=")
              .append(attr)
              .append(" expected=")
              .append(expectedVal)
              .append(" actual=")
              .append(actualVal)
              .append("\n");
        }
      }

      validated++;
      if (validated % 500 == 0) {
        LOG.info("  Validated " + validated + "/" + uniqueKeys);
      }
    }

    if (failures > 0) {
      fail(failures + " field mismatches across " + uniqueKeys + " keys:\n" + failureLog);
    }
    LOG.info(
        "All " + uniqueKeys + " keys validated successfully (" + totalRecords + " total writes)");
  }

  // --- Helper methods ---

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

  private static Map<String, AttributeValue> buildCatalogItem(
      String id,
      String chunkPath,
      long startByte,
      long endByte,
      long lastUpdatedMillis,
      int compressionVersion,
      int ttl) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("pk", AttributeValue.builder().s(id).build());
    item.put("chunk_path", AttributeValue.builder().s(chunkPath).build());
    item.put("start_byte_range", AttributeValue.builder().n(String.valueOf(startByte)).build());
    item.put("end_byte_range", AttributeValue.builder().n(String.valueOf(endByte)).build());
    item.put(
        "last_updated_millis",
        AttributeValue.builder().n(String.valueOf(lastUpdatedMillis)).build());
    item.put(
        "compression_version",
        AttributeValue.builder().n(String.valueOf(compressionVersion)).build());
    item.put("ttl", AttributeValue.builder().n(String.valueOf(ttl)).build());
    return item;
  }

  private Row readCqlRow(String table, String pkValue) {
    String ks = "alternator_" + table;
    return cqlSession
        .execute(String.format("SELECT * FROM \"%s\".\"%s\" WHERE pk = ?", ks, table), pkValue)
        .one();
  }

  private void insertViaCqlWithTimestamp(
      String targetTable, Row referenceRow, String newPk, long timestampMicros) {
    String ks = "alternator_" + targetTable;
    List<String> colNames = new ArrayList<>();
    List<Object> colValues = new ArrayList<>();

    for (ColumnDefinition col : referenceRow.getColumnDefinitions()) {
      String name = col.getName().asCql(true);
      colNames.add(name);

      if (name.equals("pk")) {
        colValues.add(newPk);
      } else {
        colValues.add(referenceRow.getObject(col.getName()));
      }
    }

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) USING TIMESTAMP %d",
            ks,
            targetTable,
            String.join(", ", colNames),
            String.join(", ", Collections.nCopies(colNames.size(), "?")),
            timestampMicros);

    cqlSession.execute(cqlSession.prepare(cql).bind(colValues.toArray()));
  }

  /**
   * Returns {@code false} and logs SEVERE if the given timestamp is in the future. This guards
   * against writing CQL rows with future timestamps that would win over all subsequent writes.
   */
  private static boolean validateTimestamp(long lastUpdatedMillis) {
    if (lastUpdatedMillis > System.currentTimeMillis()) {
      LOG.severe(
          "Rejecting timestamp "
              + lastUpdatedMillis
              + " — it is in the future (now="
              + System.currentTimeMillis()
              + ")");
      return false;
    }
    return true;
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
