import java.util.*;


// IMPORTANT -- THIS IS INDIVIDUAL WORK. ABSOLUTELY NO COLLABORATION!!!


// - implement a (main-memory) data store with OMVCC.
// - objects are <int, int> key-value pairs.
// - if an operation is to be refused by the OMVCC protocol,
//   undo its xact (what work does this take?) and throw an exception.
// - garbage collection of versions is optional.
// - throw exceptions when necessary, such as when we try to:
//   + execute an operation in a transaction that is not running
//   + read a nonexisting key
//   + delete a nonexisting key
//   + write into a key where it already has an uncommitted version
// - you may but do not need to create different exceptions for operations that
//   are refused and for operations that are refused and cause the Xact to be
//   aborted. Keep it simple!
// - keep the interface, we want to test automatically!

public class OMVCC {
  /* TODO -- your versioned key-value store data structure */

  private static long startAndCommitTimestampGen = 0;
  private static long transactionIdGen = 1L << 62;

  // returns transaction id == logical start timestamp
  public static long begin() {
    ++startAndCommitTimestampGen; //SHOULD BE USED
    return ++transactionIdGen;
  }

  // return value of object key in transaction xact
  public static int read(long xact, int key) throws Exception {
    /* TODO */
    return 0; // FIX THIS
  }

  // return the list of values of objects whose values mod k are zero.
  // this is our only kind of query / bulk read.
  public static List<Integer> modquery(long xact, int k) throws Exception {
    List<Integer> l = new ArrayList<Integer>();
    /* TODO */
    return l;
  }

  // update the value of an existing object identified by key
  // or insert <key,value> for a non-existing key in transaction xact
  public static void write(long xact, int key, int value) throws Exception {
    /* TODO */
  }

  // delete the object identified by key in transaction xact
  public static void delete(long xact, int key) throws Exception {
    /* TODO */
  }

  public static void commit(long xact)   throws Exception {
    boolean isValid = false; // FIX THIS
    /* TODO */
    if(isValid) {
      ++startAndCommitTimestampGen; //SHOULD BE USED
    }
  }

  public static void rollback(long xact) throws Exception {
    /* TODO */
  }
}
