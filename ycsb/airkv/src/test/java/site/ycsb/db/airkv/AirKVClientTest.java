//package site.ycsb.db.airkv;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import site.ycsb.ByteIterator;
//import site.ycsb.Status;
//import site.ycsb.StringByteIterator;
//import site.ycsb.workloads.CoreWorkload;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//import java.util.Vector;
//
//import static org.junit.Assert.assertEquals;
//
//public class AirKVClientTest {
//
//  @Rule
//  public TemporaryFolder tmpFolder = new TemporaryFolder();
//
//  private static final String MOCK_TABLE = "ycsb";
//  private static final String MOCK_KEY0 = "0";
//  private static final String MOCK_KEY1 = "1";
//  private static final String MOCK_KEY2 = "2";
//  private static final String MOCK_KEY3 = "3";
//  private static final int NUM_RECORDS = 10;
//  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;
//
//  private static final Map<String, ByteIterator> MOCK_DATA;
//  static {
//    MOCK_DATA = new HashMap<>(NUM_RECORDS);
//    for (int i = 0; i < NUM_RECORDS; i++) {
//      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
//    }
//  }
//
//  private AirKVClient instance;
//
//  @Before
//  public void setup() throws Exception {
//    instance = new AirKVClient();
//
//    final Properties properties = new Properties();
//    properties.setProperty(AirKVClient.PROPERTY_AIRKV_DIR, tmpFolder.getRoot().getAbsolutePath());
//    instance.setProperties(properties);
//
//    instance.init();
//  }
//
//  @After
//  public void tearDown() throws Exception {
//    instance.cleanup();
//  }
//
//  @Test
//  public void insertAndRead() throws Exception {
//    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
//    assertEquals(Status.OK, insertResult);
//
//    final Set<String> fields = MOCK_DATA.keySet();
//    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
//    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
//    assertEquals(Status.OK, readResult);
//  }
//
//
//  @Test
//  public void insertUpdateAndRead() throws Exception {
//    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);
//
//    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
//    assertEquals(Status.OK, insertResult);
//
//    for (int i = 0; i < NUM_RECORDS; i++) {
//      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
//    }
//
//    final Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
//    assertEquals(Status.OK, result);
//
//    //validate that the values changed
//    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
//    instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);
//
//    for (int i = 0; i < NUM_RECORDS; i++) {
//      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
//    }
//  }
//
//}
