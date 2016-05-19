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


	public class Pair<A, B> {
		private A first;
		private B second;

		public Pair(A first, B second) {
			super();
			this.first = first;
			this.second = second;
		}

		public int hashCode() {
			int hashFirst = first != null ? first.hashCode() : 0;
			int hashSecond = second != null ? second.hashCode() : 0;

			return (hashFirst + hashSecond) * hashSecond + hashFirst;
		}

		public boolean equals(Object other) {
			if (other instanceof Pair) {
				Pair otherPair = (Pair) other;
				return 
						((  this.first == otherPair.first ||
						( this.first != null && otherPair.first != null &&
						this.first.equals(otherPair.first))) &&
						(	this.second == otherPair.second ||
						( this.second != null && otherPair.second != null &&
						this.second.equals(otherPair.second))) );
			}

			return false;
		}

		public String toString()
		{ 
			return "(" + first + ", " + second + ")"; 
		}

		public A getFirst() {
			return first;
		}

		public void setFirst(A first) {
			this.first = first;
		}

		public B getSecond() {
			return second;
		}

		public void setSecond(B second) {
			this.second = second;
		}
	}

	private static long startAndCommitTimestampGen = 0;
	private static long transactionIdGen = 1L << 62;

	// Values committed
	static private HashMap<Integer, Integer> values = new HashMap<>();
	// Current transactions
	static private LinkedList<Transaction> listTransaction = new LinkedList<>();

	// Contain a transaction
	private static class Transaction {
		public long startTimestamp;
		public long transactionId;
		public boolean isReadOnly;
		
		public List<Pair<Integer, Integer> > modifications;
		public List< Integer > listValuesRead;

		public Transaction() {
			isReadOnly = true;
			modifications = new ArrayList<Pair<Integer,Integer>>();
			listValuesRead = new ArrayList<Integer>();
			// transaction id
			// Undo buffer
		}
	}

	static Transaction checkTransactionExist(long xact) throws Exception {
		boolean found = false;
		for (Transaction iter : listTransaction) {
			if (iter.transactionId == xact) {
				found = true;
				return iter;
			}
		}
		
		if(!found) {
			throw new Exception("Transaction doesn't exist or is terminated");
		}
		return null;
	}

	// returns transaction id == logical start timestamp
	public static long begin() {
		System.out.println(startAndCommitTimestampGen);
		System.out.println(transactionIdGen);
		
		// Creation of a new transaction with id > all other transactions
		Transaction newTransaction = new Transaction();
		
		newTransaction.startTimestamp = startAndCommitTimestampGen;
		
		// Compute an unused id (last timestamp or last id+1)
		long numberId = startAndCommitTimestampGen;
		if (!listTransaction.isEmpty()) { // Some uncommitted changes
			long lastId = listTransaction.getLast().transactionId + 1; // Because the transactions are added in the chronological order (so the last added is the last created)
			if (numberId < lastId) { // In this case, we take the max value between the last committed value and
				numberId = lastId;
			}
		}
		newTransaction.transactionId = numberId;
		
		listTransaction.add(newTransaction);
		
		++startAndCommitTimestampGen; //SHOULD BE USED
		++transactionIdGen;
		return newTransaction.transactionId;
	}

	// return value of object key in transaction xact
	public static int read(long xact, int key) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		
		// Check the key exist !! (in the committed version)
		// Get the last written version of the current transaction (or the past version)
		
		/* TODO */
		return 0; // FIX THIS
	}

	// return the list of values of objects whose values mod k are zero.
	// this is our only kind of query / bulk read.
	public static List<Integer> modquery(long xact, int k) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		if(k == 0) {
			throw new Exception("Error: modquery by 0");
		}
		
		List<Integer> l = new ArrayList<Integer>();
		/* TODO */
		return l;
	}

	// update the value of an existing object identified by key
	// or insert <key,value> for a non-existing key in transaction xact
	public static void write(long xact, int key, int value) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		currentTrans.isReadOnly = false; // At least one write operation
		/* TODO */
	}

	// delete the object identified by key in transaction xact
	public static void delete(long xact, int key) throws Exception {
		checkTransactionExist(xact);
		/* Optional */
	}

	public static void commit(long xact)   throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		boolean isValid = false; // FIX THIS
		/* TODO */
		
		// Validation phase
		if(currentTrans.isReadOnly) {
			isValid = true;
		} else {
			// Validation more in depth
		}
		
		
		if(isValid) {
			++startAndCommitTimestampGen; //SHOULD BE USED
		}
	}

	public static void rollback(long xact) throws Exception {
		checkTransactionExist(xact);
		/* TODO */
	}
}
