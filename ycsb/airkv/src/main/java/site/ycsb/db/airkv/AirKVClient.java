package site.ycsb.db.airkv;

import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * AirKV binding.
 *
 */
public class AirKVClient extends DB {
  static final String PROPERTY_AIRKV_DIR = "airkv.dir";
  static final String PROPERTY_AIRKV_DBTYPE = "airkv.dbtype";
  static final String PROPERTY_AIRKV_BLOCK_LIMIT = "airkv.block.limit";
  private static final Logger LOGGER = LoggerFactory.getLogger(AirKVClient.class);

  @GuardedBy("AirKVClient.class")
  private static int references = 0;
  @GuardedBy("AirKVClient.class")
  private static String airkvDbDir = null;
  @GuardedBy("AirKVClient.class")
  private static String dbType = null;
  @GuardedBy("AirKVClient.class")
  private static String blockNumLimit = null;
  @GuardedBy("AirKVClient.class")
  private static byte[] airkvDb = null;

  @Override
  public void init() throws DBException {
    synchronized (AirKVClient.class) {
      if (airkvDb == null) {
        airkvDbDir = getProperties().getProperty(PROPERTY_AIRKV_DIR);
        dbType = getProperties().getProperty(PROPERTY_AIRKV_DBTYPE);
        blockNumLimit = getProperties().getProperty(PROPERTY_AIRKV_BLOCK_LIMIT);
        LOGGER.info("AirKV data dir: " + airkvDbDir);

        try {
          airkvDb = RWDB.newRWDB(airkvDbDir, dbType);
          RWDB.open(airkvDb, blockNumLimit);
        } catch (final Exception e) {
          throw new DBException(e);
        }
      }

      references++;
    }

  }


  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (AirKVClient.class) {
      try {
        if (references == 1) {
          RWDB.close(airkvDb);
          airkvDb = null;
          airkvDbDir = null;
        }

      } catch (final Exception e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }

  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    //TODO: deal with fields
    try {
//      if (!COLUMN_FAMILIES.containsKey(table)) {
//        createColumnFamily(table);
//      }
//
//      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
//      final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      final byte[] values = RWDB.get(airkvDb, key.getBytes(UTF_8));
      if (values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
//    } catch(final RocksDBException e) {
    } catch (final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }


  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      // // support multiple fields
//      final Map<String, ByteIterator> result = new HashMap<>();
//      final byte[] currentValues = RWDB.get(airkvDb, key.getBytes(UTF_8));
//      if (currentValues == null) {
//        return Status.NOT_FOUND;
//      }
//      deserializeValues(currentValues, null, result);
//
//      //update
//      result.putAll(values);
//
//      //store
//      RWDB.put(airkvDb, key.getBytes(UTF_8), serializeValues(result));
//
//      return Status.OK;

      // only support single field update
      assert(values.size() == 1);
      RWDB.put(airkvDb, key.getBytes(UTF_8), serializeValues(values));
      return Status.OK;
    } catch (final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
//      if (!COLUMN_FAMILIES.containsKey(table)) {
//        createColumnFamily(table);
//      }
//
//      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
//      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(values));
      RWDB.put(airkvDb, key.getBytes(UTF_8), serializeValues(values));
      return Status.OK;
    } catch (final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }


  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }


  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

//  private byte[] serializeSingleValue(Map<String, ByteIterator> values) throws IOException {
//    assert(values.size() == 1);
//    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
//      final ByteBuffer buf = ByteBuffer.allocate(4);
//
//      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
//        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
//        final byte[] valueBytes = value.getValue().toArray();
//
//        buf.putInt(keyBytes.length);
//        baos.write(buf.array());
//        baos.write(keyBytes);
//
//        buf.clear();
//
//        buf.putInt(valueBytes.length);
//        baos.write(buf.array());
//        baos.write(valueBytes);
//
//        buf.clear();
//      }
//      return baos.toByteArray();
//    }
//  }
//
//
//  private Map<String, ByteIterator> deserializeSingleValue(final byte[] values, final Set<String> fields,
//                                                      final Map<String, ByteIterator> result) {
//    final ByteBuffer buf = ByteBuffer.allocate(4);
//
//    int offset = 0;
//    while (offset < values.length) {
//      buf.put(values, offset, 4);
//      buf.flip();
//      final int keyLen = buf.getInt();
//      buf.clear();
//      offset += 4;
//
//      final String key = new String(values, offset, keyLen);
//      offset += keyLen;
//
//      buf.put(values, offset, 4);
//      buf.flip();
//      final int valueLen = buf.getInt();
//      buf.clear();
//      offset += 4;
//
//      if (fields == null || fields.contains(key)) {
//        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
//      }
//
//      offset += valueLen;
//    }
//
//    return result;
//  }


}
