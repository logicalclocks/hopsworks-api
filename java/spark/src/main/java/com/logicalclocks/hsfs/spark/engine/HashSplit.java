/*
 *  Copyright (c) 2026. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.engine;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.transform_keys;
import static org.apache.spark.sql.functions.transform_values;
import static org.apache.spark.sql.functions.unix_date;
import static org.apache.spark.sql.functions.unix_micros;
import static org.apache.spark.sql.functions.when;

import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import scala.jdk.javaapi.CollectionConverters;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Random training dataset split by a keyed hash of each row.
 *
 * <p>Rows are serialized by their declared offline types, hashed with BLAKE2b keyed by the seed and placed by the
 * hash read as a fraction of 2^64. It must give the same bytes and splits as
 * {@code hsfs/training_dataset_split.py}, so a version splits the same rows whichever client made it.
 */
public final class HashSplit {

  private static final BigInteger TWO_POW_64 = BigInteger.ONE.shiftLeft(64);
  private static final Map<String, String> KINDS = new HashMap<>();

  static {
    for (String name : new String[] {"tinyint", "smallint", "int", "integer", "bigint"}) {
      KINDS.put(name, "int");
    }
    KINDS.put("boolean", "boolean");
    KINDS.put("float", "float");
    KINDS.put("double", "double");
    for (String name : new String[] {"string", "varchar", "char"}) {
      KINDS.put(name, "string");
    }
    KINDS.put("binary", "binary");
    KINDS.put("date", "date");
    for (String name : new String[] {"timestamp", "timestamp_ntz", "timestamp_ltz"}) {
      KINDS.put(name, "timestamp");
    }
  }

  private HashSplit() {
  }

  /**
   * Split a dataset by a keyed hash of each row.
   *
   * @param dataset the rows to split
   * @param trainingDataset the training dataset with the splits and the seed
   * @param features the feature view features, or null to hash every column
   * @return one dataset per split, in the order of the splits
   */
  public static Dataset<Row>[] split(Dataset<Row> dataset, TrainingDataset trainingDataset,
                                     List<TrainingDatasetFeature> features) {
    List<String> names = new ArrayList<>();
    List<String> types = new ArrayList<>();
    List<String> available = Arrays.asList(dataset.columns());
    if (features != null) {
      for (TrainingDatasetFeature feature : features) {
        if (available.contains(feature.getName()) && !Boolean.TRUE.equals(feature.getInferenceHelperColumn())
            && !Boolean.TRUE.equals(feature.getTrainingHelperColumn())) {
          names.add(feature.getName());
          types.add(feature.getType());
        }
      }
    }
    if (names.isEmpty()) {
      for (StructField field : dataset.schema().fields()) {
        names.add(field.name());
        types.add(null);
      }
    }
    Column[] hashed = new Column[names.size()];
    for (int i = 0; i < names.size(); i++) {
      DataType dataType = dataset.schema().apply(names.get(i)).dataType();
      if (types.get(i) == null) {
        types.set(i, dataType.simpleString());
      }
      hashed[i] = prepare(col("`" + names.get(i) + "`"), dataType).alias("c" + i);
    }
    List<Double> fractions = new ArrayList<>();
    for (Split split : trainingDataset.getSplits()) {
      // The percentage arrives as a decimal number: read it as the same double the Python client uses.
      fractions.add(Double.parseDouble(split.getPercentage().toString()));
    }
    UserDefinedFunction bucket = functions.udf(
        new Bucket(parseTypes(types), salt(trainingDataset), bounds(fractions)), DataTypes.IntegerType);
    String splitColumn = "_SPLIT_INDEX_" + UUID.randomUUID().toString().replace("-", "");
    Dataset<Row> indexed = dataset.withColumn(splitColumn, bucket.apply(struct(hashed)));
    Dataset<Row>[] result = new Dataset[fractions.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = indexed.filter(col(splitColumn).equalTo(i)).drop(splitColumn);
    }
    return result;
  }

  static byte[] salt(TrainingDataset trainingDataset) {
    if (trainingDataset.getSeed() != null) {
      return ByteBuffer.allocate(8).putLong(trainingDataset.getSeed()).array();
    }
    // Versions created before seeds were generated: fixed per version.
    return (trainingDataset.getId() + ":" + trainingDataset.getVersion()).getBytes(StandardCharsets.UTF_8);
  }

  static BigInteger[] bounds(List<Double> fractions) {
    BigInteger[] bounds = new BigInteger[fractions.size() - 1];
    double total = 0.0;
    for (int i = 0; i < bounds.length; i++) {
      total += fractions.get(i);
      bounds[i] = new BigDecimal(total).multiply(new BigDecimal(TWO_POW_64)).toBigInteger();
    }
    return bounds;
  }

  static int bucket(long hash, BigInteger[] bounds) {
    BigInteger unsigned = new BigInteger(Long.toUnsignedString(hash));
    int index = 0;
    while (index < bounds.length && bounds[index].compareTo(unsigned) <= 0) {
      index++;
    }
    return index;
  }

  /**

   * Timestamps and dates as UTC epoch microseconds and days, computed in Spark from the instant.

   */
  private static Column prepare(Column column, DataType dataType) {
    if (dataType instanceof TimestampType) {
      return unix_micros(column);
    }
    if (dataType instanceof DateType) {
      return unix_date(column);
    }
    if (!needsPrepare(dataType)) {
      return column;
    }
    if (dataType instanceof ArrayType) {
      DataType element = ((ArrayType) dataType).elementType();
      return transform(column, x -> prepare(x, element));
    }
    if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      if (needsPrepare(mapType.keyType())) {
        column = transform_keys(column, (k, v) -> prepare(k, mapType.keyType()));
      }
      if (needsPrepare(mapType.valueType())) {
        column = transform_values(column, (k, v) -> prepare(v, mapType.valueType()));
      }
      return column;
    }
    StructField[] fields = ((StructType) dataType).fields();
    Column[] prepared = new Column[fields.length];
    for (int i = 0; i < fields.length; i++) {
      prepared[i] = prepare(column.getField(fields[i].name()), fields[i].dataType()).alias(fields[i].name());
    }
    return when(column.isNull(), lit(null)).otherwise(struct(prepared));
  }

  private static boolean needsPrepare(DataType dataType) {
    if (dataType instanceof TimestampType || dataType instanceof DateType) {
      return true;
    }
    if (dataType instanceof ArrayType) {
      return needsPrepare(((ArrayType) dataType).elementType());
    }
    if (dataType instanceof MapType) {
      return needsPrepare(((MapType) dataType).keyType()) || needsPrepare(((MapType) dataType).valueType());
    }
    if (dataType instanceof StructType) {
      return Arrays.stream(((StructType) dataType).fields()).anyMatch(f -> needsPrepare(f.dataType()));
    }
    return false;
  }

  static Type[] parseTypes(List<String> types) {
    Type[] parsed = new Type[types.size()];
    for (int i = 0; i < parsed.length; i++) {
      parsed[i] = parseType(types.get(i));
    }
    return parsed;
  }

  static Type parseType(String offlineType) {
    if (offlineType == null || offlineType.isEmpty()) {
      return new Type("any", "");
    }
    int[] position = {0};
    return parseAt(offlineType, position);
  }

  private static Type parseAt(String text, int[] position) {
    int start = position[0];
    int end = start;
    while (end < text.length() && "<>,:(".indexOf(text.charAt(end)) < 0) {
      end++;
    }
    String name = text.substring(start, end).trim().toLowerCase();
    String args = "";
    if (end < text.length() && text.charAt(end) == '(') {
      int close = text.indexOf(')', end);
      args = text.substring(end + 1, close);
      end = close + 1;
    }
    position[0] = end;
    switch (name) {
      case "array": {
        position[0]++;
        Type type = new Type("array", name);
        type.element = parseAt(text, position);
        position[0]++;
        return type;
      }
      case "map": {
        position[0]++;
        Type type = new Type("map", name);
        type.key = parseAt(text, position);
        position[0]++;
        type.element = parseAt(text, position);
        position[0]++;
        return type;
      }
      case "struct": {
        position[0]++;
        Type type = new Type("struct", name);
        while (text.charAt(position[0]) != '>') {
          int colon = text.indexOf(':', position[0]);
          type.fieldNames.add(text.substring(position[0], colon).trim());
          position[0] = colon + 1;
          type.fieldTypes.add(parseAt(text, position));
          if (text.charAt(position[0]) == ',') {
            position[0]++;
          }
        }
        position[0]++;
        return type;
      }
      case "decimal": {
        Type type = new Type("decimal", name);
        type.scale = args.contains(",") ? Integer.parseInt(args.split(",")[1].trim()) : 0;
        return type;
      }
      default:
        return new Type(KINDS.getOrDefault(name, "any"), name);
    }
  }

  /**

   * A parsed offline type.

   */
  static final class Type implements Serializable {
    final String kind;
    final String name;
    int scale;
    Type key;
    Type element;
    final List<String> fieldNames = new ArrayList<>();
    final List<Type> fieldTypes = new ArrayList<>();

    Type(String kind, String name) {
      this.kind = kind;
      this.name = name;
    }
  }

  /**

   * The split index of a row, from the struct of its hashed columns.

   */
  static final class Bucket implements UDF1<Row, Integer> {
    private final Type[] types;
    private final byte[] salt;
    private final BigInteger[] bounds;

    Bucket(Type[] types, byte[] salt, BigInteger[] bounds) {
      this.types = types;
      this.salt = salt;
      this.bounds = bounds;
    }

    @Override
    public Integer call(Row row) {
      Object[] values = new Object[types.length];
      for (int i = 0; i < values.length; i++) {
        values[i] = row.get(i);
      }
      return bucket(rowHash(values, types, salt), bounds);
    }
  }

  static long rowHash(Object[] values, Type[] types, byte[] salt) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < values.length; i++) {
      encode(values[i], types[i], out);
    }
    return ByteBuffer.wrap(Blake2b.hash8(salt, out.toByteArray())).getLong();
  }

  private static boolean isMissing(Object value) {
    return value == null
        || value instanceof Double && ((Double) value).isNaN()
        || value instanceof Float && ((Float) value).isNaN();
  }

  private static IllegalArgumentException misfit(Object value, String kind) {
    return new IllegalArgumentException(
        "Cannot split on " + value + ": it does not fit the declared type `" + kind + "`.");
  }

  private static void encode(Object value, Type declared, ByteArrayOutputStream out) {
    if (isMissing(value)) {
      out.write('N');
      return;
    }
    if ("any".equals(declared.kind)) {
      declared = runtimeType(value);
    }
    switch (declared.kind) {
      case "int":
        encodeInt(asInteger(value, declared.name), out);
        break;
      case "boolean":
        out.write('B');
        out.write(asBoolean(value) ? 1 : 0);
        break;
      case "float": {
        double wide = ((Number) value).doubleValue();
        float single = ((Number) value).floatValue();
        if (Float.isInfinite(single) && !Double.isInfinite(wide)) {
          throw misfit(value, "float");
        }
        out.write('f');
        writeBytes(out, ByteBuffer.allocate(4).putInt(Float.floatToRawIntBits(single)).array());
        break;
      }
      case "double":
        out.write('F');
        writeLong(out, Double.doubleToRawLongBits(((Number) value).doubleValue()));
        break;
      case "decimal": {
        BigDecimal decimal = value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
        BigDecimal unscaled = decimal.movePointRight(declared.scale);
        if (unscaled.signum() != 0 && unscaled.stripTrailingZeros().scale() > 0) {
          throw misfit(value, "decimal(_," + declared.scale + ")");
        }
        out.write('D');
        encodeInt(unscaled.setScale(0).unscaledValue(), out);
        writeBytes(out, ByteBuffer.allocate(4).putInt(declared.scale).array());
        break;
      }
      case "string":
        writeSized(out, 'S', value.toString().getBytes(StandardCharsets.UTF_8));
        break;
      case "binary":
        writeSized(out, 'Y', (byte[]) value);
        break;
      case "date":
        out.write('d');
        writeLong(out, epochDays(value));
        break;
      case "timestamp":
        out.write('T');
        writeLong(out, epochMicros(value));
        break;
      case "array": {
        List<?> items = asList(value);
        out.write('L');
        writeBytes(out, ByteBuffer.allocate(4).putInt(items.size()).array());
        for (Object item : items) {
          encode(item, declared.element, out);
        }
        break;
      }
      case "struct":
        encodeStruct(value, declared, out);
        break;
      case "map":
        encodeMap(value, declared, out);
        break;
      default:
        throw new IllegalArgumentException("Cannot split on a value of declared type `" + declared.kind + "`.");
    }
  }

  private static void encodeStruct(Object value, Type declared, ByteArrayOutputStream out) {
    out.write('R');
    writeBytes(out, ByteBuffer.allocate(4).putInt(declared.fieldNames.size()).array());
    Row row = value instanceof Row ? (Row) value : null;
    Map<String, Integer> byName = new HashMap<>();
    if (row != null && row.schema() != null) {
      // Hive stores struct field names in lower case.
      String[] names = row.schema().fieldNames();
      for (int i = 0; i < names.length; i++) {
        byName.put(names[i].toLowerCase(), i);
      }
    }
    for (int i = 0; i < declared.fieldNames.size(); i++) {
      Object field;
      if (row != null && row.schema() != null) {
        Integer index = byName.get(declared.fieldNames.get(i).toLowerCase());
        if (index == null) {
          throw misfit(value, "struct");
        }
        field = row.get(index);
      } else if (row != null) {
        field = row.get(i);
      } else {
        field = asList(value).get(i);
      }
      encode(field, declared.fieldTypes.get(i), out);
    }
  }

  private static void encodeMap(Object value, Type declared, ByteArrayOutputStream out) {
    List<byte[][]> entries = new ArrayList<>();
    for (Map.Entry<?, ?> entry : asMap(value).entrySet()) {
      ByteArrayOutputStream key = new ByteArrayOutputStream();
      ByteArrayOutputStream item = new ByteArrayOutputStream();
      encode(entry.getKey(), declared.key, key);
      encode(entry.getValue(), declared.element, item);
      entries.add(new byte[][] {key.toByteArray(), item.toByteArray()});
    }
    entries.sort((a, b) -> {
      int byKey = Arrays.compareUnsigned(a[0], b[0]);
      return byKey != 0 ? byKey : Arrays.compareUnsigned(a[1], b[1]);
    });
    out.write('M');
    writeBytes(out, ByteBuffer.allocate(4).putInt(entries.size()).array());
    for (byte[][] entry : entries) {
      writeBytes(out, entry[0]);
      writeBytes(out, entry[1]);
    }
  }

  private static Type runtimeType(Object value) {
    if (value instanceof Boolean) {
      return new Type("boolean", "boolean");
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
        || value instanceof BigInteger) {
      return new Type("int", "bigint");
    }
    if (value instanceof Float || value instanceof Double) {
      return new Type("double", "double");
    }
    if (value instanceof String) {
      return new Type("string", "string");
    }
    if (value instanceof byte[]) {
      return new Type("binary", "binary");
    }
    if (value instanceof BigDecimal) {
      Type type = new Type("decimal", "decimal");
      type.scale = ((BigDecimal) value).scale();
      return type;
    }
    if (value instanceof Instant || value instanceof LocalDateTime || value instanceof java.sql.Timestamp) {
      return new Type("timestamp", "timestamp");
    }
    if (value instanceof LocalDate || value instanceof java.sql.Date) {
      return new Type("date", "date");
    }
    if (value instanceof Map || value instanceof scala.collection.Map || value instanceof Row) {
      Type type = new Type("map", "map");
      type.key = new Type("any", "");
      type.element = new Type("any", "");
      return type;
    }
    if (value instanceof List || value instanceof scala.collection.Seq || value instanceof Object[]) {
      Type type = new Type("array", "array");
      type.element = new Type("any", "");
      return type;
    }
    throw new IllegalArgumentException(
        "Cannot split on a value of type `" + value.getClass().getSimpleName() + "`: declare the feature type.");
  }

  private static BigInteger asInteger(Object value, String kind) {
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return BigInteger.valueOf(((Number) value).longValue());
    }
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    }
    BigDecimal decimal;
    if (value instanceof BigDecimal) {
      decimal = (BigDecimal) value;
    } else if (value instanceof Double || value instanceof Float) {
      double wide = ((Number) value).doubleValue();
      if (Double.isInfinite(wide)) {
        throw misfit(value, kind);
      }
      decimal = new BigDecimal(wide);
    } else {
      throw misfit(value, kind);
    }
    try {
      return decimal.toBigIntegerExact();
    } catch (ArithmeticException e) {
      throw misfit(value, kind);
    }
  }

  private static boolean asBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof Number) {
      double number = ((Number) value).doubleValue();
      if (number == 0 || number == 1) {
        return number == 1;
      }
    }
    throw misfit(value, "boolean");
  }

  private static long epochDays(Object value) {
    if (value instanceof Integer || value instanceof Long) {
      return ((Number) value).longValue();
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).toEpochDay();
    }
    if (value instanceof java.sql.Date) {
      return ((java.sql.Date) value).toLocalDate().toEpochDay();
    }
    throw misfit(value, "date");
  }

  private static long epochMicros(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    }
    Instant instant;
    if (value instanceof Instant) {
      instant = (Instant) value;
    } else if (value instanceof LocalDateTime) {
      // Wall time without a zone is read as UTC.
      instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
    } else if (value instanceof java.sql.Timestamp) {
      instant = ((java.sql.Timestamp) value).toInstant();
    } else {
      throw misfit(value, "timestamp");
    }
    return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
  }

  private static List<?> asList(Object value) {
    if (value instanceof List) {
      return (List<?>) value;
    }
    if (value instanceof scala.collection.Seq) {
      return CollectionConverters.asJava((scala.collection.Seq<?>) value);
    }
    if (value instanceof Object[]) {
      return Arrays.asList((Object[]) value);
    }
    if (value instanceof Collection) {
      return new ArrayList<>((Collection<?>) value);
    }
    throw misfit(value, "array");
  }

  private static Map<?, ?> asMap(Object value) {
    if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    if (value instanceof scala.collection.Map) {
      return CollectionConverters.asJava((scala.collection.Map<?, ?>) value);
    }
    if (value instanceof Row) {
      Row row = (Row) value;
      Map<Object, Object> fields = new HashMap<>();
      for (int i = 0; i < row.length(); i++) {
        fields.put(row.schema().fieldNames()[i], row.get(i));
      }
      return fields;
    }
    throw misfit(value, "map");
  }

  private static void encodeInt(BigInteger value, ByteArrayOutputStream out) {
    if (value.bitLength() <= 63) {
      out.write('I');
      writeLong(out, value.longValue());
    } else {
      writeSized(out, 'J', value.toByteArray());
    }
  }

  private static void writeSized(ByteArrayOutputStream out, char tag, byte[] raw) {
    out.write(tag);
    writeBytes(out, ByteBuffer.allocate(4).putInt(raw.length).array());
    writeBytes(out, raw);
  }

  private static void writeLong(ByteArrayOutputStream out, long value) {
    writeBytes(out, ByteBuffer.allocate(8).putLong(value).array());
  }

  private static void writeBytes(ByteArrayOutputStream out, byte[] bytes) {
    out.write(bytes, 0, bytes.length);
  }

  /**

   * BLAKE2b (RFC 7693) with a key and an 8 byte digest, as Python's {@code hashlib.blake2b}.

   */
  static final class Blake2b {
    private static final long[] IV = {
      0x6a09e667f3bcc908L, 0xbb67ae8584caa73bL, 0x3c6ef372fe94f82bL, 0xa54ff53a5f1d36f1L,
      0x510e527fade682d1L, 0x9b05688c2b3e6c1fL, 0x1f83d9abfb41bd6bL, 0x5be0cd19137e2179L
    };
    private static final int[][] SIGMA = {
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
      {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
      {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
      {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
      {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
      {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
      {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
      {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
      {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
      {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0}
    };
    private static final int DIGEST = 8;
    private static final int BLOCK = 128;

    private Blake2b() {
    }

    static byte[] hash8(byte[] key, byte[] data) {
      if (key.length > 64) {
        throw new IllegalArgumentException("A BLAKE2b key holds at most 64 bytes.");
      }
      long[] h = IV.clone();
      h[0] ^= 0x01010000L ^ ((long) key.length << 8) ^ DIGEST;
      byte[] input;
      if (key.length > 0) {
        input = new byte[BLOCK + data.length];
        System.arraycopy(key, 0, input, 0, key.length);
        System.arraycopy(data, 0, input, BLOCK, data.length);
      } else {
        input = data;
      }
      int blocks = Math.max(1, (input.length + BLOCK - 1) / BLOCK);
      for (int b = 0; b < blocks; b++) {
        byte[] block = new byte[BLOCK];
        int offset = b * BLOCK;
        int length = Math.min(BLOCK, input.length - offset);
        System.arraycopy(input, offset, block, 0, Math.max(0, length));
        boolean last = b == blocks - 1;
        compress(h, block, last ? input.length : (long) (b + 1) * BLOCK, last);
      }
      byte[] digest = new byte[DIGEST];
      for (int i = 0; i < DIGEST; i++) {
        digest[i] = (byte) (h[i / 8] >>> (8 * (i % 8)));
      }
      return digest;
    }

    private static void compress(long[] h, byte[] block, long counter, boolean last) {
      long[] m = new long[16];
      ByteBuffer buffer = ByteBuffer.wrap(block).order(java.nio.ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < 16; i++) {
        m[i] = buffer.getLong();
      }
      long[] v = new long[16];
      System.arraycopy(h, 0, v, 0, 8);
      System.arraycopy(IV, 0, v, 8, 8);
      v[12] ^= counter;
      if (last) {
        v[14] = ~v[14];
      }
      for (int round = 0; round < 12; round++) {
        int[] s = SIGMA[round % 10];
        mix(v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        mix(v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        mix(v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        mix(v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        mix(v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        mix(v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        mix(v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        mix(v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
      }
      for (int i = 0; i < 8; i++) {
        h[i] ^= v[i] ^ v[i + 8];
      }
    }

    private static void mix(long[] v, int a, int b, int c, int d, long x, long y) {
      v[a] = v[a] + v[b] + x;
      v[d] = Long.rotateRight(v[d] ^ v[a], 32);
      v[c] = v[c] + v[d];
      v[b] = Long.rotateRight(v[b] ^ v[c], 24);
      v[a] = v[a] + v[b] + y;
      v[d] = Long.rotateRight(v[d] ^ v[a], 16);
      v[c] = v[c] + v[d];
      v[b] = Long.rotateRight(v[b] ^ v[c], 63);
    }
  }
}
