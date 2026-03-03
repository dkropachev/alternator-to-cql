package com.scylladb.alternator;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * Integration test that writes all DynamoDB attribute types via Alternator, clones them to a second
 * table via CQL (the "reference-write-then-CQL-clone" technique), and reads back via Alternator to
 * verify round-trip correctness.
 *
 * <p>Each test method covers a single DynamoDB type with multiple value variants (edge cases,
 * boundary values, empty/null, unicode, etc.) and asserts that the cloned row is identical to the
 * original when read back via the DynamoDB API.
 */
public class CqlWriterTypesMappingIT {

  private DynamoDbClient dynamoClient;
  private AlternatorDynamoDbClientWrapper wrapper;
  private CqlSession cqlSession;

  /** Table A — written via Alternator API (the reference/source). */
  private String tableA;

  /** Table B — written via CQL clone (the target). */
  private String tableB;

  @Before
  public void setUp() {
    assumeTrue(
        "Integration tests disabled. Set INTEGRATION_TESTS=true to enable.",
        IntegrationTestConfig.ENABLED);

    String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    tableA = "typemap_a_" + suffix;
    tableB = "typemap_b_" + suffix;

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

  // ==================== String (S) ====================

  @Test
  public void testStringNormal() throws Exception {
    assertRoundTrip("s_normal", Map.of("val", attr().s("hello world").build()));
  }

  @Test
  public void testStringEmpty() throws Exception {
    assertRoundTrip("s_empty", Map.of("val", attr().s("").build()));
  }

  @Test
  public void testStringUnicodeLatin() throws Exception {
    assertRoundTrip("s_latin", Map.of("val", attr().s("\u00e9\u00e0\u00fc\u00f1\u00df").build()));
  }

  @Test
  public void testStringUnicodeCJK() throws Exception {
    assertRoundTrip("s_cjk", Map.of("val", attr().s("\u4e16\u754c\u4f60\u597d").build()));
  }

  @Test
  public void testStringEmoji() throws Exception {
    assertRoundTrip("s_emoji", Map.of("val", attr().s("\ud83d\ude00\ud83d\udd25\ud83c\udf0d").build()));
  }

  @Test
  public void testStringNewlinesAndTabs() throws Exception {
    assertRoundTrip("s_whitespace", Map.of("val", attr().s("line1\nline2\ttab").build()));
  }

  @Test
  public void testStringNullCharacter() throws Exception {
    assertRoundTrip("s_nullchar", Map.of("val", attr().s("before\u0000after").build()));
  }

  @Test
  public void testStringLong() throws Exception {
    String longStr = String.join("", Collections.nCopies(1000, "abcdefghij")); // 10K chars
    assertRoundTrip("s_long", Map.of("val", attr().s(longStr).build()));
  }

  @Test
  public void testStringSingleChar() throws Exception {
    assertRoundTrip("s_single", Map.of("val", attr().s("x").build()));
  }

  @Test
  public void testStringSpecialChars() throws Exception {
    assertRoundTrip(
        "s_special", Map.of("val", attr().s("quote\"slash\\colon:brace{}bracket[]").build()));
  }

  // ==================== Number (N) ====================

  @Test
  public void testNumberZero() throws Exception {
    assertRoundTrip("n_zero", Map.of("val", attr().n("0").build()));
  }

  @Test
  public void testNumberPositiveInt() throws Exception {
    assertRoundTrip("n_pos", Map.of("val", attr().n("1").build()));
  }

  @Test
  public void testNumberNegativeInt() throws Exception {
    assertRoundTrip("n_neg", Map.of("val", attr().n("-1").build()));
  }

  @Test
  public void testNumberIntMax() throws Exception {
    assertRoundTrip(
        "n_intmax", Map.of("val", attr().n(String.valueOf(Integer.MAX_VALUE)).build()));
  }

  @Test
  public void testNumberIntMin() throws Exception {
    assertRoundTrip(
        "n_intmin", Map.of("val", attr().n(String.valueOf(Integer.MIN_VALUE)).build()));
  }

  @Test
  public void testNumberLongMax() throws Exception {
    assertRoundTrip(
        "n_longmax", Map.of("val", attr().n(String.valueOf(Long.MAX_VALUE)).build()));
  }

  @Test
  public void testNumberLongMin() throws Exception {
    assertRoundTrip(
        "n_longmin", Map.of("val", attr().n(String.valueOf(Long.MIN_VALUE)).build()));
  }

  @Test
  public void testNumberLargeDecimal() throws Exception {
    assertRoundTrip(
        "n_largedec", Map.of("val", attr().n("99999999999999999999999999999999999999").build()));
  }

  @Test
  public void testNumberNegativeLargeDecimal() throws Exception {
    assertRoundTrip(
        "n_neglargedec",
        Map.of("val", attr().n("-99999999999999999999999999999999999999").build()));
  }

  @Test
  public void testNumberFloat() throws Exception {
    assertRoundTrip("n_float", Map.of("val", attr().n("3.14159").build()));
  }

  @Test
  public void testNumberNegativeFloat() throws Exception {
    assertRoundTrip("n_negfloat", Map.of("val", attr().n("-0.001").build()));
  }

  @Test
  public void testNumberVerySmallDecimal() throws Exception {
    assertRoundTrip(
        "n_smalldec", Map.of("val", attr().n("0.00000000000000000001").build()));
  }

  @Test
  public void testNumberScientificNotation() throws Exception {
    assertRoundTrip("n_sci", Map.of("val", attr().n("1E+10").build()));
  }

  @Test
  public void testNumberScientificNotationNegativeExponent() throws Exception {
    assertRoundTrip("n_scineg", Map.of("val", attr().n("5E-8").build()));
  }

  @Test
  public void testNumberTypicalTimestamp() throws Exception {
    assertRoundTrip(
        "n_timestamp", Map.of("val", attr().n("1700000000000").build()));
  }

  // ==================== Binary (B) ====================

  @Test
  public void testBinaryNormal() throws Exception {
    byte[] data = "binary data here".getBytes(StandardCharsets.UTF_8);
    assertRoundTrip(
        "b_normal", Map.of("val", attr().b(SdkBytes.fromByteArray(data)).build()));
  }

  @Test
  public void testBinarySingleByte() throws Exception {
    assertRoundTrip(
        "b_single", Map.of("val", attr().b(SdkBytes.fromByteArray(new byte[] {0x42})).build()));
  }

  @Test
  public void testBinaryEmpty() throws Exception {
    assertRoundTrip(
        "b_empty", Map.of("val", attr().b(SdkBytes.fromByteArray(new byte[0])).build()));
  }

  @Test
  public void testBinaryAllZeros() throws Exception {
    assertRoundTrip(
        "b_zeros", Map.of("val", attr().b(SdkBytes.fromByteArray(new byte[256])).build()));
  }

  @Test
  public void testBinaryAllOnes() throws Exception {
    byte[] ones = new byte[256];
    Arrays.fill(ones, (byte) 0xFF);
    assertRoundTrip(
        "b_ones", Map.of("val", attr().b(SdkBytes.fromByteArray(ones)).build()));
  }

  @Test
  public void testBinaryFullByteRange() throws Exception {
    byte[] allBytes = new byte[256];
    for (int i = 0; i < 256; i++) {
      allBytes[i] = (byte) i;
    }
    assertRoundTrip(
        "b_fullrange", Map.of("val", attr().b(SdkBytes.fromByteArray(allBytes)).build()));
  }

  @Test
  public void testBinaryLarge() throws Exception {
    byte[] large = new byte[64 * 1024]; // 64KB
    new Random(42).nextBytes(large);
    assertRoundTrip(
        "b_large", Map.of("val", attr().b(SdkBytes.fromByteArray(large)).build()));
  }

  // ==================== Boolean (BOOL) ====================

  @Test
  public void testBooleanTrue() throws Exception {
    assertRoundTrip("bool_true", Map.of("val", attr().bool(true).build()));
  }

  @Test
  public void testBooleanFalse() throws Exception {
    assertRoundTrip("bool_false", Map.of("val", attr().bool(false).build()));
  }

  // ==================== Null (NULL) ====================

  @Test
  public void testNullType() throws Exception {
    assertRoundTrip("null_val", Map.of("val", attr().nul(true).build()));
  }

  // ==================== String Set (SS) ====================

  @Test
  public void testStringSetMultiple() throws Exception {
    assertRoundTrip(
        "ss_multi", Map.of("val", attr().ss("alpha", "beta", "gamma").build()));
  }

  @Test
  public void testStringSetSingle() throws Exception {
    assertRoundTrip("ss_single", Map.of("val", attr().ss("only_one").build()));
  }

  @Test
  public void testStringSetWithUnicode() throws Exception {
    assertRoundTrip(
        "ss_unicode",
        Map.of("val", attr().ss("\u00e9l\u00e8ve", "\u4e16\u754c", "plain").build()));
  }

  @Test
  public void testStringSetWithEmptyString() throws Exception {
    assertRoundTrip(
        "ss_empty_elem", Map.of("val", attr().ss("", "notempty").build()));
  }

  @Test
  public void testStringSetLarge() throws Exception {
    List<String> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      items.add("item_" + i);
    }
    assertRoundTrip(
        "ss_large", Map.of("val", attr().ss(items).build()));
  }

  // ==================== Number Set (NS) ====================

  @Test
  public void testNumberSetIntegers() throws Exception {
    assertRoundTrip(
        "ns_ints", Map.of("val", attr().ns("1", "2", "3", "100").build()));
  }

  @Test
  public void testNumberSetDecimals() throws Exception {
    assertRoundTrip(
        "ns_dec", Map.of("val", attr().ns("1.5", "2.7", "3.14").build()));
  }

  @Test
  public void testNumberSetMixed() throws Exception {
    assertRoundTrip(
        "ns_mixed", Map.of("val", attr().ns("0", "-1", "999999999999", "0.001").build()));
  }

  @Test
  public void testNumberSetSingle() throws Exception {
    assertRoundTrip("ns_single", Map.of("val", attr().ns("42").build()));
  }

  // ==================== Binary Set (BS) ====================

  @Test
  public void testBinarySetMultiple() throws Exception {
    assertRoundTrip(
        "bs_multi",
        Map.of(
            "val",
            attr()
                .bs(
                    SdkBytes.fromByteArray(new byte[] {0x01, 0x02}),
                    SdkBytes.fromByteArray(new byte[] {0x03, 0x04}))
                .build()));
  }

  @Test
  public void testBinarySetSingle() throws Exception {
    assertRoundTrip(
        "bs_single",
        Map.of("val", attr().bs(SdkBytes.fromByteArray(new byte[] {(byte) 0xAA, (byte) 0xBB})).build()));
  }

  @Test
  public void testBinarySetVaryingSizes() throws Exception {
    assertRoundTrip(
        "bs_sizes",
        Map.of(
            "val",
            attr()
                .bs(
                    SdkBytes.fromByteArray(new byte[] {0x01}),
                    SdkBytes.fromByteArray(new byte[] {0x02, 0x03, 0x04, 0x05}),
                    SdkBytes.fromByteArray(new byte[64]))
                .build()));
  }

  // ==================== List (L) ====================

  @Test
  public void testListEmpty() throws Exception {
    assertRoundTrip(
        "l_empty", Map.of("val", attr().l(Collections.emptyList()).build()));
  }

  @Test
  public void testListSingleString() throws Exception {
    assertRoundTrip(
        "l_single", Map.of("val", attr().l(attr().s("only").build()).build()));
  }

  @Test
  public void testListMixedTypes() throws Exception {
    assertRoundTrip(
        "l_mixed",
        Map.of(
            "val",
            attr()
                .l(
                    attr().s("text").build(),
                    attr().n("42").build(),
                    attr().bool(true).build(),
                    attr().nul(true).build(),
                    attr().b(SdkBytes.fromByteArray(new byte[] {0x01})).build())
                .build()));
  }

  @Test
  public void testListNested() throws Exception {
    assertRoundTrip(
        "l_nested",
        Map.of(
            "val",
            attr()
                .l(
                    attr().s("outer").build(),
                    attr()
                        .l(attr().s("inner1").build(), attr().s("inner2").build())
                        .build())
                .build()));
  }

  @Test
  public void testListDeeplyNested() throws Exception {
    // 4 levels deep
    AttributeValue deepest = attr().s("deep").build();
    AttributeValue level3 = attr().l(deepest).build();
    AttributeValue level2 = attr().l(level3).build();
    AttributeValue level1 = attr().l(level2).build();
    assertRoundTrip("l_deep", Map.of("val", level1));
  }

  @Test
  public void testListOfMaps() throws Exception {
    assertRoundTrip(
        "l_maps",
        Map.of(
            "val",
            attr()
                .l(
                    attr().m(Map.of("k1", attr().s("v1").build())).build(),
                    attr().m(Map.of("k2", attr().n("2").build())).build())
                .build()));
  }

  @Test
  public void testListLarge() throws Exception {
    List<AttributeValue> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      items.add(attr().s("item_" + i).build());
    }
    assertRoundTrip("l_large", Map.of("val", attr().l(items).build()));
  }

  // ==================== Map (M) ====================

  @Test
  public void testMapEmpty() throws Exception {
    assertRoundTrip(
        "m_empty", Map.of("val", attr().m(Collections.emptyMap()).build()));
  }

  @Test
  public void testMapSingleEntry() throws Exception {
    assertRoundTrip(
        "m_single", Map.of("val", attr().m(Map.of("key", attr().s("value").build())).build()));
  }

  @Test
  public void testMapMixedTypes() throws Exception {
    Map<String, AttributeValue> mapVal = new HashMap<>();
    mapVal.put("str_key", attr().s("text").build());
    mapVal.put("num_key", attr().n("42").build());
    mapVal.put("bool_key", attr().bool(true).build());
    mapVal.put("null_key", attr().nul(true).build());
    mapVal.put("bin_key", attr().b(SdkBytes.fromByteArray(new byte[] {0x01})).build());
    assertRoundTrip("m_mixed", Map.of("val", attr().m(mapVal).build()));
  }

  @Test
  public void testMapNested() throws Exception {
    assertRoundTrip(
        "m_nested",
        Map.of(
            "val",
            attr()
                .m(
                    Map.of(
                        "outer",
                        attr()
                            .m(Map.of("inner", attr().s("deep_value").build()))
                            .build()))
                .build()));
  }

  @Test
  public void testMapDeeplyNested() throws Exception {
    // 4 levels deep
    AttributeValue deepest = attr().m(Map.of("d", attr().s("bottom").build())).build();
    AttributeValue level3 = attr().m(Map.of("c", deepest)).build();
    AttributeValue level2 = attr().m(Map.of("b", level3)).build();
    AttributeValue level1 = attr().m(Map.of("a", level2)).build();
    assertRoundTrip("m_deep", Map.of("val", level1));
  }

  @Test
  public void testMapWithList() throws Exception {
    assertRoundTrip(
        "m_list",
        Map.of(
            "val",
            attr()
                .m(
                    Map.of(
                        "items",
                        attr().l(attr().s("a").build(), attr().s("b").build()).build()))
                .build()));
  }

  @Test
  public void testMapWithSets() throws Exception {
    Map<String, AttributeValue> mapVal = new HashMap<>();
    mapVal.put("ss", attr().ss("x", "y").build());
    mapVal.put("ns", attr().ns("1", "2").build());
    assertRoundTrip("m_sets", Map.of("val", attr().m(mapVal).build()));
  }

  @Test
  public void testMapUnicodeKeys() throws Exception {
    Map<String, AttributeValue> mapVal = new HashMap<>();
    mapVal.put("\u00e9l\u00e8ve", attr().s("student").build());
    mapVal.put("\u4e16\u754c", attr().s("world").build());
    assertRoundTrip("m_unikeys", Map.of("val", attr().m(mapVal).build()));
  }

  @Test
  public void testMapLarge() throws Exception {
    Map<String, AttributeValue> mapVal = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      mapVal.put("key_" + i, attr().s("value_" + i).build());
    }
    assertRoundTrip("m_large", Map.of("val", attr().m(mapVal).build()));
  }

  // ==================== Multiple attributes in one item ====================

  @Test
  public void testAllTypesInSingleItem() throws Exception {
    createTables();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("pk", attr().s("all_types").build());
    item.put("string_attr", attr().s("hello").build());
    item.put("number_attr", attr().n("42").build());
    item.put("binary_attr", attr().b(SdkBytes.fromUtf8String("binary")).build());
    item.put("bool_attr", attr().bool(true).build());
    item.put("null_attr", attr().nul(true).build());
    item.put("string_set_attr", attr().ss("a", "b", "c").build());
    item.put("number_set_attr", attr().ns("1", "2", "3").build());
    item.put(
        "binary_set_attr",
        attr()
            .bs(
                SdkBytes.fromByteArray(new byte[] {0x01}),
                SdkBytes.fromByteArray(new byte[] {0x02}))
            .build());
    item.put(
        "list_attr",
        attr().l(attr().s("item1").build(), attr().n("2").build()).build());
    item.put(
        "map_attr",
        attr().m(Map.of("key1", attr().s("val1").build())).build());

    dynamoClient.putItem(PutItemRequest.builder().tableName(tableA).item(item).build());

    cloneViaCql("all_types");

    Map<String, AttributeValue> fromA = readViaAlternator(tableA, "all_types");
    Map<String, AttributeValue> fromB = readViaAlternator(tableB, "all_types");

    assertNotNull("Item should exist in table A", fromA);
    assertNotNull("Item should exist in table B (CQL-cloned)", fromB);

    assertItemsEqual("all_types", fromA, fromB);
  }

  @Test
  public void testMultipleAttributesSameType() throws Exception {
    Map<String, AttributeValue> attrs = new HashMap<>();
    attrs.put("name", attr().s("Alice").build());
    attrs.put("email", attr().s("alice@example.com").build());
    attrs.put("city", attr().s("").build());
    attrs.put("bio", attr().s("Line1\nLine2").build());
    assertRoundTrip("multi_str", attrs);
  }

  @Test
  public void testComplexNestedStructure() throws Exception {
    // Simulates a realistic complex DynamoDB item
    Map<String, AttributeValue> address =
        Map.of(
            "street", attr().s("123 Main St").build(),
            "city", attr().s("Springfield").build(),
            "zip", attr().s("62701").build());

    Map<String, AttributeValue> attrs = new HashMap<>();
    attrs.put("name", attr().s("Bob").build());
    attrs.put("age", attr().n("35").build());
    attrs.put("active", attr().bool(true).build());
    attrs.put("address", attr().m(address).build());
    attrs.put("tags", attr().ss("admin", "user").build());
    attrs.put(
        "scores",
        attr().l(attr().n("100").build(), attr().n("95").build(), attr().n("88").build()).build());
    attrs.put(
        "metadata",
        attr()
            .m(
                Map.of(
                    "created", attr().n("1700000000000").build(),
                    "flags",
                        attr().l(attr().bool(true).build(), attr().bool(false).build()).build()))
            .build());

    assertRoundTrip("complex", attrs);
  }

  // ==================== CQL schema verification ====================

  @Test
  public void testPartitionKeyIsCqlText() throws Exception {
    createTables();

    putItem(tableA, "pk_check", Map.of("val", attr().s("test").build()));

    String keyspace = "alternator_" + tableA;
    Row row =
        cqlSession
            .execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE pk = ?", keyspace, tableA),
                "pk_check")
            .one();

    assertNotNull("Row should exist", row);
    ColumnDefinition pkCol = row.getColumnDefinitions().get("pk");
    assertNotNull("pk column should exist", pkCol);
    assertEquals("Partition key (S) should map to CQL text", DataTypes.TEXT, pkCol.getType());
  }

  // --- Core round-trip assertion ---

  /**
   * Writes an item to table A via Alternator, clones it to table B via CQL, reads both back via
   * Alternator, and asserts all non-key attributes are identical.
   */
  private void assertRoundTrip(String pk, Map<String, AttributeValue> extraAttrs) throws Exception {
    createTables();

    putItem(tableA, pk, extraAttrs);

    cloneViaCql(pk);

    Map<String, AttributeValue> fromA = readViaAlternator(tableA, pk);
    Map<String, AttributeValue> fromB = readViaAlternator(tableB, pk);

    assertNotNull("Item pk=" + pk + " should exist in table A", fromA);
    assertNotNull("Item pk=" + pk + " should exist in table B (CQL-cloned)", fromB);

    assertItemsEqual(pk, fromA, fromB);
  }

  private void assertItemsEqual(
      String pk, Map<String, AttributeValue> fromA, Map<String, AttributeValue> fromB) {
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
              + " should match between Alternator-written and CQL-cloned",
          fromA.get(key),
          fromB.get(key));
    }
  }

  /**
   * Reads a row from table A via CQL and inserts it into table B via CQL with the same partition
   * key.
   */
  private void cloneViaCql(String pk) {
    String srcKs = "alternator_" + tableA;
    String tgtKs = "alternator_" + tableB;

    Row srcRow =
        cqlSession
            .execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE pk = ?", srcKs, tableA), pk)
            .one();
    assertNotNull("Source row pk=" + pk + " should be readable via CQL", srcRow);

    List<String> colNames = new ArrayList<>();
    List<Object> colValues = new ArrayList<>();

    for (ColumnDefinition col : srcRow.getColumnDefinitions()) {
      String name = col.getName().asCql(true);
      colNames.add(name);
      colValues.add(srcRow.getObject(col.getName()));
    }

    String cql =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)",
            tgtKs,
            tableB,
            String.join(", ", colNames),
            String.join(", ", Collections.nCopies(colNames.size(), "?")));

    cqlSession.execute(cqlSession.prepare(cql).bind(colValues.toArray()));
  }

  // --- Helpers ---

  private boolean tablesCreated = false;

  private void createTables() throws InterruptedException {
    if (tablesCreated) return;
    createTable(tableA);
    createTable(tableB);
    tablesCreated = true;
  }

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
            .keySchema(
                KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
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

  private void putItem(String table, String pk, Map<String, AttributeValue> extraAttrs) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("pk", attr().s(pk).build());
    item.putAll(extraAttrs);
    dynamoClient.putItem(PutItemRequest.builder().tableName(table).item(item).build());
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

  private static AttributeValue.Builder attr() {
    return AttributeValue.builder();
  }
}
