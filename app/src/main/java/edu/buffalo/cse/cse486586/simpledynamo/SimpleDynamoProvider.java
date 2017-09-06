package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.MatrixCursor;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.util.Set;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;



public class SimpleDynamoProvider extends ContentProvider {

	public static GroupMessengerProviderHelper createDB;
	//String t, int i, boolean p, ConcurrentHashMap<String, String> m
	public static  Message mMessage = new Message("", 0, false, null);
	public static String tableName= "";


	class GroupMessengerProviderHelper extends SQLiteOpenHelper {

		public GroupMessengerProviderHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
			super(context, name, factory, version);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onCreate(db);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("DROP TABLE IF EXISTS " + tableName);
			db.execSQL("CREATE TABLE  IF NOT EXISTS " + tableName + " (key TEXT, value TEXT);");

		}


	}


	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final int SERVER_PORT = 10000;

	public static SimpleDynamoProvider sPartitionManager = new SimpleDynamoProvider();
	public static Vector<ConcurrentHashMap<String,String>> keyStore =
			new Vector<ConcurrentHashMap<String,String>>(3);
	public static ConcurrentHashMap<String,String> sMap = new ConcurrentHashMap<String, String>();
	public static AtomicInteger sReceived = new AtomicInteger(0);
	public static ConcurrentHashMap<String, Object> sWaitSet =
			new ConcurrentHashMap<String, Object>();
	public static boolean sRecover = false;
	public static int mPort;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub


			if (sRecover) {
				recoverMeNow();
			}

			ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
			map.put(selection, "");


			int port = 0;
			try {
				port = sPartitionManager.getPort(genHash(selection));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if (port == mPort) {
				keyStore.get(0).remove(selection);

				SQLiteDatabase database = SimpleDynamoProvider.createDB.getWritableDatabase();
				String [] selectionArgs1 ={selection};
				database.delete(SimpleDynamoProvider.tableName , "key = ?",selectionArgs1);

				Log.e(TAG , "Removed key is " + selection);

				ClientTask.ClientMessage cM = new ClientTask.ClientMessage(sPartitionManager.nextPort(port), "delete", 1, map);
				ClientTask.client(cM);







			} else {


				ClientTask.ClientMessage cM = new ClientTask.ClientMessage(port, "delete", 0, map);
				ClientTask.client(cM);


			}

			return 0;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

			try {
				// Wait to recover
				if (sRecover) {
					synchronized (sReceived) {
						recoverMeNow();
					}
				}


				String [] selectionArgs ={values.getAsString("key")};

				SQLiteDatabase database = createDB.getWritableDatabase();
				Cursor curso = database.query(tableName, null , "key = ?",selectionArgs,null,null,null);
				int count = curso.getCount();
				if(count<1)
					database.insert(tableName, null ,values);
				else
					database.update(tableName ,values,"key = ?", selectionArgs );

				ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
				map.put(values.getAsString("key"), values.getAsString("value"));

				int port = 0;
				port = sPartitionManager.getPort(genHash(values.getAsString("key")));


				if (port == mPort) {
					keyStore.get(0).putAll(map);

					ClientTask.ClientMessage cM = new ClientTask.ClientMessage(sPartitionManager.nextPort(port), "insert",1, map);
					ClientTask.client(cM);


				} else {

					ClientTask.ClientMessage cM = new ClientTask.ClientMessage(port, "insert", 0, map);
					ClientTask.client(cM);


				}

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			return uri;

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub


			tableName = "table_"+mPort;
			createDB = new GroupMessengerProviderHelper(getContext(),tableName,null,1);
			TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
					Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			mPort = Integer.parseInt(portStr) * 2;

			createKeyStore();


			SharedPreferences failCheck = this.getContext().getSharedPreferences("failCheck", 0);
			if (failCheck.getBoolean("initialStartUp", true)) {
				failCheck.edit().putBoolean("initialStartUp", false).commit();
			} else {
				sRecover = true;
			}


			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(SERVER_PORT);
			} catch (IOException e) {

				Log.e(TAG, "Failed to create socket" + e.getMessage());
				e.printStackTrace();
			}


			ServerTask.server(serverSocket);

			if (sRecover) {

				int port = sPartitionManager.nextPort(mPort);

				recoverDataStore(port);
			}

			return true;

	}




	public static void recoverDataStore(int port){

		int port1 = sPartitionManager.nextPort(mPort);

		ClientTask.ClientMessage clientMessage = new ClientTask.ClientMessage(port, "recover", 2, null);
		ClientTask.client(clientMessage);




		port = sPartitionManager.nextPort(port);
		ClientTask.ClientMessage clientMessage1 = new ClientTask.ClientMessage(port, "recover", 2, null);
		ClientTask.client(clientMessage1);


		port = sPartitionManager.prevPort(mPort);
		//ClientTask.client(port, "recover", 1, null);

		ClientTask.ClientMessage clientMessage2 = new ClientTask.ClientMessage(port, "recover", 1, null);
		ClientTask.client(clientMessage2);

	}





	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub


		if (sRecover) {

			recoverMeNow();

		}

		synchronized (this) {

			if (selection.equals("@")) {
				//add everything from the keyStore
				return getAll(selection);
			} else if (selection.equalsIgnoreCase("*")) {
				//add everything from the all nodes
				return getStar(selection);

			}
			else{
				return getSelectKey(selection);
			}
		}

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


	public int getPort(String hashKey) {
		if(hashKey.compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1")<=0) return 11124;
		else if(hashKey.compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") <=0) return 11112;
		else if(hashKey.compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") <=0 ) return 11108;
		else if(hashKey.compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643")<=0) return 11116;
		else if(hashKey.compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12")<=0) return 11120;
		return 11124;

	}

	public int getQueryPort(String hashKey) {
		int currPort =  getPort(hashKey);
		int next1 = nextPort(currPort);
		int next2 = nextPort(next1);
		return  next2;
	}


	public int nextPort(int port) {
		if(port == 11124)
			return 11112;
		else if(port == 11112)
			return 11108;
		else if(port == 11108)
			return 11116;
		else if(port == 11116)
			return 11120;
		else if(port == 11120)
			return 11124;
		return 0;
	}


	public int prevPort(int port) {
		if(port == 11124)
			return 11120;
		else if(port == 11112)
			return 11124;
		else if(port == 11108)
			return 11112;
		else if(port == 11116)
			return 11108;
		else if(port == 11120)
			return 11116;
		return 0;
	}


	private void createKeyStore() {

		keyStore.add(0, new ConcurrentHashMap<String, String>());
		keyStore.add(1, new ConcurrentHashMap<String, String>());
		keyStore.add(2, new ConcurrentHashMap<String, String>());
	}


	private Cursor getSelectKey(String selection) {




		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
		ConcurrentHashMap<String, String> map = null;
		int port = 0;

		sMap.clear();
		try {
			port = sPartitionManager.getQueryPort(genHash(selection));
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Error in creating hash key" + e.getMessage());
		}

		if (port == mPort) {
			String value = keyStore.get(2).get(selection);
			if (value == null) {
				Object object = sWaitSet.get(selection);
				if (object == null) {
					object = new Object();
					sWaitSet.put(selection, object);
				}
				synchronized (object) {
					try {

						object.wait();

					} catch (InterruptedException e) {
						Log.e(TAG, "Sync error in query");
					}
				}
				value = keyStore.get(2).get(selection);
			}

			cursor.addRow(new String[] {selection, value});
			SQLiteDatabase database = createDB.getWritableDatabase();
			String [] selectionArgs1 ={selection};
			Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

			Log.v("query", selection);


		} else {
			map = new ConcurrentHashMap<String, String>();
			map.put(selection, "");


			ClientTask.ClientMessage cM = new ClientTask.ClientMessage(port, "query", 2, map);
			ClientTask.client(cM);

			synchronized (sMap) {
				try {
					sMap.wait();
				} catch (InterruptedException e) {
					Log.e(TAG, "Query interrupted");
				}
			}
			String[] row = null;
			for (Entry<String, String> entry : sMap.entrySet()) {
				row = new String[] {entry.getKey(), entry.getValue()};
				cursor.addRow(row);
				SQLiteDatabase database = createDB.getWritableDatabase();
				String [] selectionArgs1 ={selection};
				Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

				Log.v("query", selection);
			}

		}
		return cursor;

	}

	private Cursor getStar(String selection) {
		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
		sMap.clear();
		Vector<Integer> v = new Vector<Integer>();
		v.add(11124);v.add(11112);v.add(11108);v.add(11116);v.add(11120);
		for (Integer p : v) {
			if (p == mPort) {
				sMap.putAll(keyStore.get(2));
				sReceived.incrementAndGet();
			} else {
				ClientTask.ClientMessage cM = new ClientTask.ClientMessage(p, "queryAll", 2, null);
				ClientTask.client(cM);
			}
		}

		recoverMeNow();

		Set<String> keyMap = sMap.keySet();

		for(String keyMapKey : keyMap) {
			String cursorEntry[] = {keyMapKey,  sMap.get(keyMapKey)};
			cursor.addRow(cursorEntry);
		}





		SQLiteDatabase database = createDB.getWritableDatabase();
		String [] selectionArgs1 ={selection};
		Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

		Log.v("query", selection);
		return cursor;
	}

	public static void recoverMeNow() {
		synchronized (sReceived) {
			try {
				sReceived.wait();
			} catch (InterruptedException e) {
				Log.e(TAG, "Failed to recover" + e.getMessage());
				e.printStackTrace();
			}

		}
	}

	private Cursor getAll(String selection) {

		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});

		for (int i = 0; i < 3; i = i + 1) {

			ConcurrentHashMap<String,String> currMap = keyStore.get(i);
			Set<String> keyMap = currMap.keySet();

			for(String keyMapKey : keyMap) {
				String cursorEntry[] = {keyMapKey,  currMap.get(keyMapKey)};
				cursor.addRow(cursorEntry);
			}


			SQLiteDatabase database = createDB.getWritableDatabase();
			String [] selectionArgs1 ={selection};
			Cursor curso = database.query(tableName, null , "key = ?",selectionArgs1,null,null,null);

			Log.v("query", selection);

		}
		return cursor;
	}


}
