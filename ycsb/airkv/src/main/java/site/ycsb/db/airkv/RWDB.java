package site.ycsb.db.airkv;


/**
 * RWDB for jni.
 */
public final class RWDB {
  // This declares that the static `hello` method will be provided
  // a native library.
//  private static native ByteBuffer newRWDB(String path, String dbtype);
//  private static native boolean put(ByteBuffer dbRef, String key, String value);
//  private static native byte[] get(ByteBuffer dbRef, String key);
  private long dbHandler;

  private RWDB() {
    //not called
  }

  public static native byte[] newRWDB(String path, String dbtype);

  //  public static native boolean open(ByteBuffer dbRef, HashMap<String, String> props);
  //TODO: add properties
//  public static native void open(RWDB dbRef) throws AirKVException;
  public static native void open(byte[] dbRef, String blockNumLimit) throws AirKVException;

  public static native void close(byte[] dbRef) throws AirKVException;

  public static native void put(byte[] dbRef, byte[] key, byte[] value) throws AirKVException;

  public static native byte[] get(byte[] dbRef, byte[] key) throws AirKVException;

  static {
    // This actually loads the shared object that we'll be creating.
    // The actual location of the .so or .dll may differ based on your
    // platform.
    System.loadLibrary("airkv");
  }


  // The rest is just regular ol' Java!
  public static void main(String[] args) {
    try {
//      byte[] dbRef = RWDB.newRWDB("file:///tmp/xxx/a", "RemoteFakeStore");
      byte[] dbRef = RWDB.newRWDB("az:///integration/", "AzureStore");
      RWDB.open(dbRef, "50000");
      RWDB.put(dbRef, "1".getBytes(), "Robin".getBytes());
      byte[] res = RWDB.get(dbRef, "1".getBytes());
      System.out.println("the value for key 1 is: " + new String(res));
    } catch (AirKVException e) {
      e.printStackTrace();
    }

  }
}
