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


	public static class Pair<A, B> {
		// Taken from stackoverflow
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
	static private HashMap<Integer, Value> values = new HashMap<>();
	// Current transactions
	static private LinkedList<Transaction> listTransaction = new LinkedList<>();

	// Rollback the transaction
	static void cancelAndThrow(Transaction trans, String message) throws Exception {
		// rollback(trans.transactionId); Useless ??? Because we remove the transaction just after
		listTransaction.remove(trans); // We remove the erroneous transaction
		throw new Exception(message);
	}
	
	// Contain a transaction
	private static class Transaction {
		public long startTimestamp;
		public long transactionId;
		public boolean isReadOnly;
		public boolean hasModquery = false;
		
		//public List<Pair<Integer, Integer> > modifications;
		//public List< Integer > listValuesRead;
		
		public HashMap<Integer, Value> valuesModified = new HashMap<>();
		public LinkedList<Integer> keysRead = new LinkedList<>();

		public Transaction() {
			isReadOnly = true;
			//modifications = new ArrayList<Pair<Integer,Integer>>();
			//listValuesRead = new ArrayList<Integer>();
			// transaction id
			// Undo buffer
		}

		public int read(int key) throws Exception {
			// Return the last version of the database (if that one is not updated)
			
			// If we did modify the value
			if(valuesModified.containsKey(key)) {
				return valuesModified.get(key).getLast(); // Last written value
			}
			else if(values.containsKey(key)) { // Otherwise, we get the last 
				return values.get(key).getLast(this, this.startTimestamp);
			} else { // The key does not exist
				cancelAndThrow(this, "Error: Trying to read an non initialized value");
				return 0; // Never reached (Exception throw on the previous line)
			}
		}
	}
	
	// Contain a transaction
	private static class Value {
		public LinkedList<Pair<Long, Integer> > modifHist;

		public Value() {
			modifHist = new LinkedList<>();
		}
		
		public int getLast() { // Should be called only on the transaction value if we are sure that it has been modified
			return modifHist.getLast().second; // Return the last value
		}
		
		public int getLast(Transaction trans, long startTimestamp) throws Exception {
			
			boolean found = false;
			int returnValue = 0;
			for (Pair<Long, Integer> 	iter : modifHist) {
				if(iter.first < startTimestamp) { // It exist a more recent value
					found = true;
					returnValue = iter.second;
				}
			}
			
			if(!found) {
				cancelAndThrow(trans, "Error: The value you're trying to read has been initialized after the start timestamp");
			}
			
			return returnValue;
		}

		void addValue(long timestamp, int newValue) {
			modifHist.add(new Pair<Long, Integer>(timestamp, newValue));
		}

		public long getLastTimestamp() {
			return modifHist.getLast().first; // Return the last timestamp
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
		//System.out.println(startAndCommitTimestampGen);
		//System.out.println(transactionIdGen);
		
		// Creation of a new transaction with id > all other transactions
		Transaction newTransaction = new Transaction();
		
		newTransaction.startTimestamp = startAndCommitTimestampGen;
		newTransaction.transactionId = transactionIdGen;
		
		listTransaction.add(newTransaction);
		
		++startAndCommitTimestampGen; //SHOULD BE USED
		++transactionIdGen;
		
		return newTransaction.transactionId;
	}

	// return value of object key in transaction xact
	public static int read(long xact, int key) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		
		currentTrans.keysRead.add(key); // To check when validating there is no conflict
		
		// Check the key exist !! (in the committed version)
		// Get the last written version of the current transaction (or the past version)
		return currentTrans.read(key);
	}

	// return the list of values of objects whose values mod k are zero.
	// this is our only kind of query / bulk read.
	public static List<Integer> modquery(long xact, int k) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		if(k == 0) {
			cancelAndThrow(currentTrans, "Error: modquery by 0");
		}
		
		// For each value
		
		List<Integer> l = new ArrayList<Integer>();
		
		//System.out.println("Get all dividers");
		HashMap<Integer, Value> mergedMap = new HashMap();
		mergedMap.putAll(values);
		mergedMap.putAll(currentTrans.valuesModified); // Will replace all previous values modified
		for (Map.Entry<Integer, Value> iter : mergedMap.entrySet()) {
			// We get the most recent one
			int value = iter.getValue().getLast();
			if(value % k == 0)
			{
				//System.out.println("Add " + iter.getKey() + " : " + value);
				l.add(value);
			}
		}
		
		currentTrans.hasModquery = true;

		return l;
	}

	// update the value of an existing object identified by key
	// or insert <key,value> for a non-existing key in transaction xact
	public static void write(long xact, int key, int value) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		currentTrans.isReadOnly = false; // At least one write operation
		
		// Check that we have the right to write
		
		boolean isValid = true;
		String writingError = "";
		// Throw an exception and abort if:
		// 1) There exist another uncommitted version of the key
		for (Transaction iterTrans : listTransaction) {
			if(iterTrans.transactionId != currentTrans.transactionId) { // We check both are different
				if(iterTrans.valuesModified.containsKey(key)) { // The key exist in another transaction
					isValid = false;
					writingError = "uncommited version";
				}
			}
		}
		// 2) There exist a more recent committed version of the key
		if (values.containsKey(key)) {
			if (values.get(key).getLastTimestamp() > currentTrans.startTimestamp) { // Someone did commit a newer version
				isValid = false;
				writingError = "more recent committed version";
			}
		}
		
		if(isValid) {
			// If we have the right, then we update the value
			
			// If the key is new, we create a new value
			if (!currentTrans.valuesModified.containsKey(key)) {
				currentTrans.valuesModified.put(key, new Value());
			}
			
			// We update the value
			currentTrans.valuesModified.get(key).addValue( // On the right key
					0, // We don't care (Right ????)
					value // The last value we have written
				);
		} else { // Otherwise: cancel !!!
			cancelAndThrow(currentTrans, "Error: Cannot write the key (" + writingError + ")");
		}
	}

	// delete the object identified by key in transaction xact
	public static void delete(long xact, int key) throws Exception {
		checkTransactionExist(xact);
		/* Optional */
	}

	public static void commit(long xact)   throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		boolean isValid = true; // FIX THIS
		/* TODO */
		
		// Validation phase
		if(currentTrans.isReadOnly) {
			isValid = true;
		} else {
			// Validation more in depth
			
			// Check correctness for the modquery operation
			if (currentTrans.hasModquery) {
				for (Map.Entry<Integer, Value> entry : values.entrySet()) { // For each key
					for (Pair<Long, Integer> iter : entry.getValue().modifHist) { // We get the history
						if (iter.first > currentTrans.startTimestamp) { // The key has been modified during the current transaction lifetime
							//System.out.println("Conflict with a recent written value");
							isValid = false;
						}
					}
				}
			}
			// Check correctness for read operations
			for (Integer key : currentTrans.keysRead) { // For each key we read 
				for (Pair<Long, Integer> iter : values.get(key).modifHist) { // We check the key history
					if (iter.first > currentTrans.startTimestamp) { // The key has been modified during the current transaction lifetime
						//System.out.println("Conflict with a recent written value");
						isValid = false;
					}
				}
			}
			
			//isValid = true;
		}
		
		
		if(isValid) {

			
			//System.out.println("Commiting...");
			// If everything is ok, then we commit
			for (Map.Entry<Integer, Value> entry : currentTrans.valuesModified.entrySet()) { // Is empty for read-only transactions
				//System.out.println("Update value " + entry.getKey());
				// If the key is new, we create a new value
				if (!values.containsKey(entry.getKey())) {
					values.put(entry.getKey(), new Value());
				}
				
				// We update the value
				values.get(entry.getKey()).addValue( // On the right key
						startAndCommitTimestampGen, // We update at the next timestamp
						entry.getValue().getLast() // The last value we have written
					);
				//System.out.println("New value: " + values.get(entry.getKey()).getLast());
			}
			
			listTransaction.remove(currentTrans); // The transaction is closed!!
			++startAndCommitTimestampGen; //SHOULD BE USED
		}
		else {
			cancelAndThrow(currentTrans, "Error: validation error");
		}
	}

	public static void rollback(long xact) throws Exception {
		Transaction currentTrans = checkTransactionExist(xact);
		
		// TODO: Clear buffer ?? < Should be automatically done by the garbage collector
		
		listTransaction.remove(currentTrans);
	}
}
