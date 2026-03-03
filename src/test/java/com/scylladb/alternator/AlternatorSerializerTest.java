package com.scylladb.alternator;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Unit tests for {@link AlternatorSerializer} — verifies that DynamoDB {@link AttributeValue}
 * objects are serialized to exactly the same bytes that Alternator stores in the CQL {@code :attrs}
 * map.
 *
 * <p>No Alternator or CQL connection needed — all assertions are against expected byte patterns.
 */
public class AlternatorSerializerTest {

  // ==================== String (S) — tag 0x00 ====================

  @Test
  public void testStringSimple() {
    byte[] result = serialize(attr().s("hello").build());
    assertTag(0x00, result);
    assertPayloadEquals("hello", result);
  }

  @Test
  public void testStringEmpty() {
    byte[] result = serialize(attr().s("").build());
    assertTag(0x00, result);
    assertEquals("Empty string: tag only", 1, result.length);
  }

  @Test
  public void testStringSingleChar() {
    byte[] result = serialize(attr().s("x").build());
    assertTag(0x00, result);
    assertPayloadEquals("x", result);
  }

  @Test
  public void testStringUnicodeLatin() {
    String s = "\u00e9\u00e0\u00fc\u00f1\u00df"; // éàüñß
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringUnicodeCJK() {
    String s = "\u4e16\u754c\u4f60\u597d"; // 世界你好
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringEmoji() {
    String s = "\ud83d\ude00\ud83d\udd25\ud83c\udf0d"; // 😀🔥🌍
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringNewlinesAndTabs() {
    String s = "line1\nline2\ttab";
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringNullCharacter() {
    String s = "before\u0000after";
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringLong() {
    String s = String.join("", Collections.nCopies(1000, "abcdefghij")); // 10K chars
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  @Test
  public void testStringSpecialChars() {
    String s = "quote\"slash\\colon:brace{}bracket[]";
    byte[] result = serialize(attr().s(s).build());
    assertTag(0x00, result);
    assertPayloadEquals(s, result);
  }

  // ==================== Number (N) — tag 0x03 ====================

  @Test
  public void testNumberZero() {
    byte[] result = serialize(attr().n("0").build());
    assertTag(0x03, result);
    assertNumberPayload("0", result);
  }

  @Test
  public void testNumberPositiveInt() {
    byte[] result = serialize(attr().n("1").build());
    assertTag(0x03, result);
    assertNumberPayload("1", result);
  }

  @Test
  public void testNumberNegativeInt() {
    byte[] result = serialize(attr().n("-1").build());
    assertTag(0x03, result);
    assertNumberPayload("-1", result);
  }

  @Test
  public void testNumberIntMax() {
    String n = String.valueOf(Integer.MAX_VALUE);
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberIntMin() {
    String n = String.valueOf(Integer.MIN_VALUE);
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberLongMax() {
    String n = String.valueOf(Long.MAX_VALUE);
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberLongMin() {
    String n = String.valueOf(Long.MIN_VALUE);
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberLargeDecimal() {
    String n = "99999999999999999999999999999999999999";
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberNegativeLargeDecimal() {
    String n = "-99999999999999999999999999999999999999";
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberFloat() {
    byte[] result = serialize(attr().n("3.14159").build());
    assertTag(0x03, result);
    assertNumberPayload("3.14159", result);
  }

  @Test
  public void testNumberNegativeFloat() {
    byte[] result = serialize(attr().n("-0.001").build());
    assertTag(0x03, result);
    assertNumberPayload("-0.001", result);
  }

  @Test
  public void testNumberVerySmallDecimal() {
    String n = "0.00000000000000000001";
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberScientificNotation() {
    // 1E+10 = 10000000000
    byte[] result = serialize(attr().n("1E+10").build());
    assertTag(0x03, result);
    assertNumberPayload("1E+10", result);
  }

  @Test
  public void testNumberScientificNotationNegativeExponent() {
    // 5E-8 = 0.00000005
    byte[] result = serialize(attr().n("5E-8").build());
    assertTag(0x03, result);
    assertNumberPayload("5E-8", result);
  }

  @Test
  public void testNumberTypicalTimestamp() {
    String n = "1700000000000";
    byte[] result = serialize(attr().n(n).build());
    assertTag(0x03, result);
    assertNumberPayload(n, result);
  }

  @Test
  public void testNumberDecimalEncoding() {
    // Verify the exact byte layout: tag(1) + scale(4 BE) + unscaled(BE BigInteger)
    byte[] result = serialize(attr().n("12.34").build());
    assertTag(0x03, result);

    // BigDecimal("12.34") → scale=2, unscaledValue=1234
    assertEquals("Scale byte 0", 0x00, result[1]);
    assertEquals("Scale byte 1", 0x00, result[2]);
    assertEquals("Scale byte 2", 0x00, result[3]);
    assertEquals("Scale byte 3", 0x02, result[4]);

    // 1234 = 0x04D2 → big-endian two's complement
    byte[] expectedUnscaled = new BigInteger("1234").toByteArray();
    byte[] actualUnscaled = Arrays.copyOfRange(result, 5, result.length);
    assertArrayEquals("Unscaled value bytes", expectedUnscaled, actualUnscaled);
  }

  @Test
  public void testNumberNegativeDecimalEncoding() {
    byte[] result = serialize(attr().n("-5.5").build());
    assertTag(0x03, result);

    // BigDecimal("-5.5") → scale=1, unscaledValue=-55
    assertEquals("Scale byte 3", 0x01, result[4]);

    byte[] expectedUnscaled = new BigInteger("-55").toByteArray();
    byte[] actualUnscaled = Arrays.copyOfRange(result, 5, result.length);
    assertArrayEquals("Unscaled value bytes for -55", expectedUnscaled, actualUnscaled);
  }

  // ==================== Binary (B) — tag 0x01 ====================

  @Test
  public void testBinaryNormal() {
    byte[] data = "binary data here".getBytes(StandardCharsets.UTF_8);
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(data)).build());
    assertTag(0x01, result);
    assertArrayEquals("Binary payload", data, Arrays.copyOfRange(result, 1, result.length));
  }

  @Test
  public void testBinarySingleByte() {
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(new byte[] {0x42})).build());
    assertTag(0x01, result);
    assertEquals("Length = tag + 1 byte", 2, result.length);
    assertEquals("Payload byte", 0x42, result[1]);
  }

  @Test
  public void testBinaryEmpty() {
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(new byte[0])).build());
    assertTag(0x01, result);
    assertEquals("Empty binary: tag only", 1, result.length);
  }

  @Test
  public void testBinaryAllZeros() {
    byte[] data = new byte[256];
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(data)).build());
    assertTag(0x01, result);
    assertEquals("Length = tag + 256", 257, result.length);
    assertArrayEquals("All zeros payload", data, Arrays.copyOfRange(result, 1, result.length));
  }

  @Test
  public void testBinaryAllOnes() {
    byte[] data = new byte[256];
    Arrays.fill(data, (byte) 0xFF);
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(data)).build());
    assertTag(0x01, result);
    assertArrayEquals("All 0xFF payload", data, Arrays.copyOfRange(result, 1, result.length));
  }

  @Test
  public void testBinaryFullByteRange() {
    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) data[i] = (byte) i;
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(data)).build());
    assertTag(0x01, result);
    assertArrayEquals(
        "Full byte range payload", data, Arrays.copyOfRange(result, 1, result.length));
  }

  @Test
  public void testBinaryLarge() {
    byte[] data = new byte[64 * 1024]; // 64KB
    new Random(42).nextBytes(data);
    byte[] result = serialize(attr().b(SdkBytes.fromByteArray(data)).build());
    assertTag(0x01, result);
    assertEquals("Length = tag + 64KB", 1 + 64 * 1024, result.length);
    assertArrayEquals("Large binary payload", data, Arrays.copyOfRange(result, 1, result.length));
  }

  // ==================== Boolean (BOOL) — tag 0x02 ====================

  @Test
  public void testBooleanTrue() {
    byte[] result = serialize(attr().bool(true).build());
    assertTag(0x02, result);
    assertEquals("Boolean length = tag + 1 byte", 2, result.length);
    assertEquals("True = 0x01", 0x01, result[1]);
  }

  @Test
  public void testBooleanFalse() {
    byte[] result = serialize(attr().bool(false).build());
    assertTag(0x02, result);
    assertEquals("Boolean length = tag + 1 byte", 2, result.length);
    assertEquals("False = 0x00", 0x00, result[1]);
  }

  // ==================== Null (NULL) — tag 0x04 + JSON ====================

  @Test
  public void testNull() {
    byte[] result = serialize(attr().nul(true).build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"NULL\":true}", result);
  }

  // ==================== String Set (SS) — tag 0x04 + JSON ====================

  @Test
  public void testStringSetMultiple() {
    byte[] result = serialize(attr().ss("alpha", "beta", "gamma").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should start with {\"SS\":[", json.startsWith("{\"SS\":["));
    assertTrue("Should contain \"alpha\"", json.contains("\"alpha\""));
    assertTrue("Should contain \"beta\"", json.contains("\"beta\""));
    assertTrue("Should contain \"gamma\"", json.contains("\"gamma\""));
  }

  @Test
  public void testStringSetSingle() {
    byte[] result = serialize(attr().ss("only_one").build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"SS\":[\"only_one\"]}", result);
  }

  @Test
  public void testStringSetWithUnicode() {
    byte[] result = serialize(attr().ss("\u00e9l\u00e8ve", "\u4e16\u754c", "plain").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain unicode strings", json.contains("\u00e9l\u00e8ve"));
    assertTrue("Should contain CJK", json.contains("\u4e16\u754c"));
  }

  @Test
  public void testStringSetWithEmptyElement() {
    byte[] result = serialize(attr().ss("", "notempty").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain empty string", json.contains("\"\""));
  }

  @Test
  public void testStringSetWithSpecialChars() {
    byte[] result = serialize(attr().ss("with\"quote", "with\\slash").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should have escaped quote", json.contains("\\\""));
    assertTrue("Should have escaped backslash", json.contains("\\\\"));
  }

  @Test
  public void testStringSetLarge() {
    List<String> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) items.add("item_" + i);
    byte[] result = serialize(attr().ss(items).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain first item", json.contains("\"item_0\""));
    assertTrue("Should contain last item", json.contains("\"item_99\""));
  }

  // ==================== Number Set (NS) — tag 0x04 + JSON ====================

  @Test
  public void testNumberSetIntegers() {
    byte[] result = serialize(attr().ns("1", "2", "3", "100").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should start with {\"NS\":[", json.startsWith("{\"NS\":["));
    assertTrue("Should contain \"1\"", json.contains("\"1\""));
    assertTrue("Should contain \"100\"", json.contains("\"100\""));
  }

  @Test
  public void testNumberSetDecimals() {
    byte[] result = serialize(attr().ns("1.5", "2.7", "3.14").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain \"3.14\"", json.contains("\"3.14\""));
  }

  @Test
  public void testNumberSetMixed() {
    byte[] result = serialize(attr().ns("0", "-1", "999999999999", "0.001").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain \"-1\"", json.contains("\"-1\""));
    assertTrue("Should contain \"0.001\"", json.contains("\"0.001\""));
  }

  @Test
  public void testNumberSetSingle() {
    byte[] result = serialize(attr().ns("42").build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"NS\":[\"42\"]}", result);
  }

  // ==================== Binary Set (BS) — tag 0x04 + JSON ====================

  @Test
  public void testBinarySetMultiple() {
    byte[] result =
        serialize(
            attr()
                .bs(
                    SdkBytes.fromByteArray(new byte[] {0x01, 0x02}),
                    SdkBytes.fromByteArray(new byte[] {0x03, 0x04}))
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should start with {\"BS\":[", json.startsWith("{\"BS\":["));
    // 0x01,0x02 → base64 "AQI=", 0x03,0x04 → base64 "AwQ="
    assertTrue("Should contain base64 of [01,02]", json.contains("\"AQI=\""));
    assertTrue("Should contain base64 of [03,04]", json.contains("\"AwQ=\""));
  }

  @Test
  public void testBinarySetSingle() {
    byte[] result =
        serialize(attr().bs(SdkBytes.fromByteArray(new byte[] {(byte) 0xAA, (byte) 0xBB})).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should start with {\"BS\":[", json.startsWith("{\"BS\":["));
  }

  @Test
  public void testBinarySetVaryingSizes() {
    byte[] result =
        serialize(
            attr()
                .bs(
                    SdkBytes.fromByteArray(new byte[] {0x01}),
                    SdkBytes.fromByteArray(new byte[] {0x02, 0x03, 0x04, 0x05}),
                    SdkBytes.fromByteArray(new byte[64]))
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should be valid BS JSON", json.startsWith("{\"BS\":["));
  }

  // ==================== List (L) — tag 0x04 + JSON ====================

  @Test
  public void testListEmpty() {
    byte[] result = serialize(attr().l(Collections.emptyList()).build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"L\":[]}", result);
  }

  @Test
  public void testListSingleString() {
    byte[] result = serialize(attr().l(attr().s("only").build()).build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"L\":[{\"S\":\"only\"}]}", result);
  }

  @Test
  public void testListMixedTypes() {
    byte[] result =
        serialize(
            attr()
                .l(
                    attr().s("text").build(),
                    attr().n("42").build(),
                    attr().bool(true).build(),
                    attr().nul(true).build(),
                    attr().b(SdkBytes.fromByteArray(new byte[] {0x01})).build())
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain {\"S\":\"text\"}", json.contains("{\"S\":\"text\"}"));
    assertTrue("Should contain {\"N\":\"42\"}", json.contains("{\"N\":\"42\"}"));
    assertTrue("Should contain {\"BOOL\":true}", json.contains("{\"BOOL\":true}"));
    assertTrue("Should contain {\"NULL\":true}", json.contains("{\"NULL\":true}"));
    assertTrue("Should contain {\"B\":\"AQ==\"}", json.contains("{\"B\":\"AQ==\"}"));
  }

  @Test
  public void testListNested() {
    byte[] result =
        serialize(
            attr()
                .l(
                    attr().s("outer").build(),
                    attr().l(attr().s("inner1").build(), attr().s("inner2").build()).build())
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain nested list", json.contains("{\"L\":[{\"S\":\"inner1\"}"));
  }

  @Test
  public void testListDeeplyNested() {
    // 4 levels deep
    AttributeValue deepest = attr().s("deep").build();
    AttributeValue level3 = attr().l(deepest).build();
    AttributeValue level2 = attr().l(level3).build();
    AttributeValue level1 = attr().l(level2).build();
    byte[] result = serialize(level1);
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain deeply nested value", json.contains("\"deep\""));
    // {L:[{L:[{L:[{S:"deep"}]}]}]}
    assertTrue(
        "Should have nested L", json.contains("{\"L\":[{\"L\":[{\"L\":[{\"S\":\"deep\"}]}]}]}"));
  }

  @Test
  public void testListOfMaps() {
    byte[] result =
        serialize(
            attr()
                .l(
                    attr().m(Map.of("k1", attr().s("v1").build())).build(),
                    attr().m(Map.of("k2", attr().n("2").build())).build())
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain map with k1", json.contains("\"k1\""));
    assertTrue("Should contain map with k2", json.contains("\"k2\""));
  }

  @Test
  public void testListLarge() {
    List<AttributeValue> items = new ArrayList<>();
    for (int i = 0; i < 100; i++) items.add(attr().s("item_" + i).build());
    byte[] result = serialize(attr().l(items).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain first item", json.contains("\"item_0\""));
    assertTrue("Should contain last item", json.contains("\"item_99\""));
  }

  // ==================== Map (M) — tag 0x04 + JSON ====================

  @Test
  public void testMapEmpty() {
    byte[] result = serialize(attr().m(Collections.emptyMap()).build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"M\":{}}", result);
  }

  @Test
  public void testMapSingleEntry() {
    byte[] result = serialize(attr().m(Map.of("key", attr().s("value").build())).build());
    assertTag(0x04, result);
    assertJsonPayloadEquals("{\"M\":{\"key\":{\"S\":\"value\"}}}", result);
  }

  @Test
  public void testMapMixedTypes() {
    Map<String, AttributeValue> mapVal = new LinkedHashMap<>();
    mapVal.put("str_key", attr().s("text").build());
    mapVal.put("num_key", attr().n("42").build());
    mapVal.put("bool_key", attr().bool(true).build());
    mapVal.put("null_key", attr().nul(true).build());
    byte[] result = serialize(attr().m(mapVal).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain string entry", json.contains("\"str_key\":{\"S\":\"text\"}"));
    assertTrue("Should contain number entry", json.contains("\"num_key\":{\"N\":\"42\"}"));
    assertTrue("Should contain bool entry", json.contains("\"bool_key\":{\"BOOL\":true}"));
    assertTrue("Should contain null entry", json.contains("\"null_key\":{\"NULL\":true}"));
  }

  @Test
  public void testMapNested() {
    byte[] result =
        serialize(
            attr()
                .m(
                    Map.of(
                        "outer", attr().m(Map.of("inner", attr().s("deep_value").build())).build()))
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue(
        "Should contain nested map",
        json.contains("\"outer\":{\"M\":{\"inner\":{\"S\":\"deep_value\"}}}"));
  }

  @Test
  public void testMapDeeplyNested() {
    AttributeValue deepest = attr().m(Map.of("d", attr().s("bottom").build())).build();
    AttributeValue level3 = attr().m(Map.of("c", deepest)).build();
    AttributeValue level2 = attr().m(Map.of("b", level3)).build();
    AttributeValue level1 = attr().m(Map.of("a", level2)).build();
    byte[] result = serialize(level1);
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain deeply nested value", json.contains("\"bottom\""));
  }

  @Test
  public void testMapWithList() {
    byte[] result =
        serialize(
            attr()
                .m(Map.of("items", attr().l(attr().s("a").build(), attr().s("b").build()).build()))
                .build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain list in map", json.contains("\"items\":{\"L\":["));
  }

  @Test
  public void testMapWithSets() {
    Map<String, AttributeValue> mapVal = new LinkedHashMap<>();
    mapVal.put("ss", attr().ss("x", "y").build());
    mapVal.put("ns", attr().ns("1", "2").build());
    byte[] result = serialize(attr().m(mapVal).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain SS", json.contains("\"SS\""));
    assertTrue("Should contain NS", json.contains("\"NS\""));
  }

  @Test
  public void testMapUnicodeKeys() {
    Map<String, AttributeValue> mapVal = new LinkedHashMap<>();
    mapVal.put("\u00e9l\u00e8ve", attr().s("student").build());
    mapVal.put("\u4e16\u754c", attr().s("world").build());
    byte[] result = serialize(attr().m(mapVal).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain unicode key", json.contains("\u00e9l\u00e8ve"));
    assertTrue("Should contain CJK key", json.contains("\u4e16\u754c"));
  }

  @Test
  public void testMapLarge() {
    Map<String, AttributeValue> mapVal = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) mapVal.put("key_" + i, attr().s("value_" + i).build());
    byte[] result = serialize(attr().m(mapVal).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should contain first key", json.contains("\"key_0\""));
    assertTrue("Should contain last key", json.contains("\"key_99\""));
  }

  // ==================== JSON escaping in fallback types ====================

  @Test
  public void testJsonEscapingInMapKeys() {
    byte[] result =
        serialize(attr().m(Map.of("key\"with\"quotes", attr().s("val").build())).build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should escape quotes in key", json.contains("key\\\"with\\\"quotes"));
  }

  @Test
  public void testJsonEscapingInStringSetValues() {
    byte[] result = serialize(attr().ss("has\"quote", "has\\slash").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should escape quote", json.contains("has\\\"quote"));
    assertTrue("Should escape backslash", json.contains("has\\\\slash"));
  }

  @Test
  public void testJsonEscapingControlChars() {
    byte[] result = serialize(attr().ss("tab\there", "newline\nhere").build());
    assertTag(0x04, result);
    String json = jsonPayload(result);
    assertTrue("Should escape tab", json.contains("\\t"));
    assertTrue("Should escape newline", json.contains("\\n"));
  }

  // ==================== Attribute name serialization ====================

  @Test
  public void testAttributeNameSimple() {
    byte[] result = AlternatorSerializer.serializeAttributeName("myAttribute");
    assertArrayEquals("myAttribute".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testAttributeNameUnicode() {
    byte[] result = AlternatorSerializer.serializeAttributeName("\u4e16\u754c");
    assertArrayEquals("\u4e16\u754c".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testAttributeNameEmpty() {
    byte[] result = AlternatorSerializer.serializeAttributeName("");
    assertEquals("Empty name = empty bytes", 0, result.length);
  }

  // ==================== Number deserialization ====================

  @Test
  public void testDeserializeNumberZero() {
    byte[] serialized = serialize(attr().n("0").build());
    ByteBuffer buf = ByteBuffer.wrap(serialized, 1, serialized.length - 1);
    assertEquals("0", AlternatorSerializer.deserializeNumber(buf));
  }

  @Test
  public void testDeserializeNumberPositive() {
    byte[] serialized = serialize(attr().n("12345").build());
    ByteBuffer buf = ByteBuffer.wrap(serialized, 1, serialized.length - 1);
    assertEquals("12345", AlternatorSerializer.deserializeNumber(buf));
  }

  @Test
  public void testDeserializeNumberNegative() {
    byte[] serialized = serialize(attr().n("-99").build());
    ByteBuffer buf = ByteBuffer.wrap(serialized, 1, serialized.length - 1);
    assertEquals("-99", AlternatorSerializer.deserializeNumber(buf));
  }

  @Test
  public void testDeserializeNumberDecimal() {
    byte[] serialized = serialize(attr().n("3.14").build());
    ByteBuffer buf = ByteBuffer.wrap(serialized, 1, serialized.length - 1);
    assertEquals("3.14", AlternatorSerializer.deserializeNumber(buf));
  }

  @Test
  public void testDeserializeNumberRoundTrip() {
    String[] numbers = {
      "0",
      "1",
      "-1",
      "42",
      "3.14159",
      "-0.001",
      "99999999999999999999999999999999999999",
      "0.00000000000000000001",
      "1700000000000"
    };
    for (String n : numbers) {
      byte[] serialized = serialize(attr().n(n).build());
      ByteBuffer buf = ByteBuffer.wrap(serialized, 1, serialized.length - 1);
      String deserialized = AlternatorSerializer.deserializeNumber(buf);
      assertEquals(
          "Round-trip for " + n,
          new BigDecimal(n).stripTrailingZeros().toPlainString(),
          deserialized);
    }
  }

  // ==================== Combined: all types produce correct tags ====================

  @Test
  public void testAllTypeTagValues() {
    assertEquals("String tag", 0x00, serialize(attr().s("x").build())[0]);
    assertEquals(
        "Binary tag", 0x01, serialize(attr().b(SdkBytes.fromByteArray(new byte[] {1})).build())[0]);
    assertEquals("Boolean tag", 0x02, serialize(attr().bool(true).build())[0]);
    assertEquals("Number tag", 0x03, serialize(attr().n("1").build())[0]);
    assertEquals("Null tag", 0x04, serialize(attr().nul(true).build())[0]);
    assertEquals("SS tag", 0x04, serialize(attr().ss("a").build())[0]);
    assertEquals("NS tag", 0x04, serialize(attr().ns("1").build())[0]);
    assertEquals(
        "BS tag", 0x04, serialize(attr().bs(SdkBytes.fromByteArray(new byte[] {1})).build())[0]);
    assertEquals("List tag", 0x04, serialize(attr().l(Collections.emptyList()).build())[0]);
    assertEquals("Map tag", 0x04, serialize(attr().m(Collections.emptyMap()).build())[0]);
  }

  // ==================== Helpers ====================

  private static byte[] serialize(AttributeValue value) {
    return AlternatorSerializer.serializeValue(value);
  }

  private static AttributeValue.Builder attr() {
    return AttributeValue.builder();
  }

  private static void assertTag(int expectedTag, byte[] result) {
    assertTrue("Result should not be empty", result.length > 0);
    assertEquals("Type tag", expectedTag, result[0] & 0xFF);
  }

  private static void assertPayloadEquals(String expected, byte[] result) {
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    byte[] actual = Arrays.copyOfRange(result, 1, result.length);
    assertArrayEquals("Payload bytes", expectedBytes, actual);
  }

  private static void assertJsonPayloadEquals(String expectedJson, byte[] result) {
    String actual = jsonPayload(result);
    assertEquals("JSON payload", expectedJson, actual);
  }

  private static String jsonPayload(byte[] result) {
    return new String(result, 1, result.length - 1, StandardCharsets.UTF_8);
  }

  private static void assertNumberPayload(String numberStr, byte[] result) {
    BigDecimal bd = new BigDecimal(numberStr);
    int scale = bd.scale();
    byte[] unscaled = bd.unscaledValue().toByteArray();

    // Verify scale (4 bytes big-endian at offset 1)
    int actualScale =
        ((result[1] & 0xFF) << 24)
            | ((result[2] & 0xFF) << 16)
            | ((result[3] & 0xFF) << 8)
            | (result[4] & 0xFF);
    assertEquals("Scale for " + numberStr, scale, actualScale);

    // Verify unscaled value
    byte[] actualUnscaled = Arrays.copyOfRange(result, 5, result.length);
    assertArrayEquals("Unscaled value for " + numberStr, unscaled, actualUnscaled);
  }
}
