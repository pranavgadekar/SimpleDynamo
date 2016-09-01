package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static final int N = 3;
	// Ports
	private static final ArrayList<Integer> REMOTE_PORTS = new ArrayList<Integer>(){{add(11108);add(11112);add(11116);add(11120);add(11124);}};

	private static final String KEY = "key";
	private static final String VALUE = "value";

	private static final byte INSERT = 0;
	private static final byte QUERY = 1;
	private static final byte QUERY_ALL = 2;
	private static final byte DELETE = 3;
	private static final byte RECOVER = 5;

	private static Context context;

	public static CircularLinkedList circularLinkedList = new CircularLinkedList();
	public static List<ConcurrentHashMap<String,String>> allMessages = new ArrayList<ConcurrentHashMap<String, String>>(N);
	public static ConcurrentHashMap<String,String> localMessages = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, Object> queryMessages = new ConcurrentHashMap<String, Object>();
	public static AtomicInteger notifyFlag = new AtomicInteger(0);
	public static boolean recoverFlag = false;
	public static int myPort;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		int recoveryStatus = waitForRecovery();
		if(recoveryStatus ==1){
			Log.e(TAG,"Recovery Successful in Delete block");
		}else {
			Log.e(TAG,"Recovery failed in Delete block");
		}

		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put(selection, "");

		try {
			int port = circularLinkedList.getPort(genHash(selection));

			if (port != myPort) {
				ClientThread clientThread = new ClientThread(myPort, port, DELETE, 0, map);
				Thread thread = new Thread(clientThread);
				thread.start();
				thread.join();
			} else {
				allMessages.get(0).remove(selection);
				ClientThread clientThread = new ClientThread(myPort, circularLinkedList.getNextNode(port), DELETE, 1, map);
				Thread thread = new Thread(clientThread);
				thread.start();
				thread.join();
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Failed to generate Hash of Key in Delete block");
		} catch (InterruptedException e) {
			Log.e(TAG,"InterruptedException in Delete block");
		}
		return 0;
	}

	public static int waitForRecovery() {
		int returnValue = 1;
		if (recoverFlag) {
			synchronized (notifyFlag) {
				try {
					notifyFlag.wait();
					Log.e(TAG,"Recovery successful");
				} catch (InterruptedException e) {
					returnValue = -1;
					Log.e(TAG, "Recovery failed");

				}
			}
		}
		return returnValue;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		int recoveryStatus = waitForRecovery();
		if(recoveryStatus ==1){
			Log.e(TAG,"Recovery Successful in Insert block");
		}else {
			Log.e(TAG,"Recovery failed in Insert block");
		}

		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put(values.getAsString(KEY), values.getAsString(VALUE));

		try {
			int port = circularLinkedList.getPort(genHash(values.getAsString(KEY)));

			if (port != myPort) {
				ClientThread clientThread = new ClientThread(myPort, port, INSERT, 0, map);
				Thread thread = new Thread(clientThread);
				thread.start();
				thread.join();
			} else {
				allMessages.get(0).putAll(map);
				ClientThread clientThread = new ClientThread(myPort, circularLinkedList.getNextNode(port), INSERT, 1, map);
				Thread thread = new Thread(clientThread);
				thread.start();
				thread.join();
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Failed to generate Hash of Key to Insert");
		} catch (InterruptedException e) {
			Log.e(TAG, "InterruptedException in Insert Block");
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
				Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = Integer.parseInt(portStr) * 2;

		try {
			for(Integer remote_port : REMOTE_PORTS){
				circularLinkedList.add(genHash(String.valueOf(remote_port / 2)), remote_port);
			}

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Failed to generate Hash of Port in Circular LinkedList structure");
			return false;
		}

		for (int i=0;i< N;i++){
			allMessages.add(i,new ConcurrentHashMap<String, String>());
		}

		BufferedReader bufferedReader = null;
		context = getContext();

		try {
			File file = new File(context.getFilesDir().getAbsolutePath());
			bufferedReader = new BufferedReader(new FileReader(file + "/" + "RECOVER"));
			String value  = bufferedReader.readLine();
			bufferedReader.close();
			recoverFlag = true;
		} catch (Exception e){
			try {

				File file = new File(context.getFilesDir().getAbsolutePath(), "RECOVER");
				FileWriter fileWrite = new FileWriter(file);
				fileWrite.write("RECOVER");
				fileWrite.close();
				recoverFlag = false;

			} catch (Exception e1) {
				e.printStackTrace();
			}
		}

		ServerThread server = new ServerThread();
		Thread serverThread = new Thread(server);
		serverThread.start();

		if (recoverFlag) {
			// Get 1
			int port = circularLinkedList.getNextNode(myPort);
			ClientThread clientThread = new ClientThread(myPort, port, RECOVER, 2, null);
			Thread thread = new Thread(clientThread);
			thread.start();

			port = circularLinkedList.getNextNode(circularLinkedList.getNextNode(myPort));
			ClientThread clientThreadOne = new ClientThread(myPort, port, RECOVER, 2, null);
			Thread threadOne = new Thread(clientThreadOne);
			threadOne.start();

			port = circularLinkedList.getPrevNode(myPort);
			ClientThread clientThreadTwo = new ClientThread(myPort, port, RECOVER, 1, null);
			Thread threadTwo = new Thread(clientThreadTwo);
			threadTwo.start();
		}

		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		int recoveryStatus = waitForRecovery();
		if(recoveryStatus ==1){
			Log.e(TAG,"Recovery Successful in Query block");
		}else {
			Log.e(TAG,"Recovery failed in Query block");
		}

		synchronized (this) {
			MatrixCursor cursor;
			cursor = new MatrixCursor(new String[] {KEY, VALUE});
			ConcurrentHashMap<String, String> map = null;
			int port = 0;

			if (selection.equalsIgnoreCase("*")){
				localMessages.clear();
				List<Integer> allPorts = circularLinkedList.getAllNodes();

				for (Integer p : allPorts) {
					if (p != myPort) {
						ClientThread clientThread = new ClientThread(myPort, p, QUERY_ALL, N - 1, null);
						Thread thread = new Thread(clientThread);
						thread.start();
					} else {
						localMessages.putAll(allMessages.get(N - 1));
						notifyFlag.incrementAndGet();
					}
				}

				synchronized (notifyFlag) {
					try {
						notifyFlag.wait();
					} catch (InterruptedException e) {
						Log.e(TAG, "InterruptedException in Query All Block");
					}
				}

				for (String key : localMessages.keySet()){
					cursor.addRow(new String[] {key,localMessages.get(key)});
				}
			}
			else if (selection.equalsIgnoreCase("@")) {
				for (int i = 0; i < N; i++) {
					for(String key : allMessages.get(i).keySet()){
						cursor.addRow(new String[] {key,allMessages.get(i).get(key)});
					}
				}
			}
			else {
				localMessages.clear();
				try {
					port = circularLinkedList.getQueryPort(genHash(selection));
				} catch (NoSuchAlgorithmException e) {
					Log.e(TAG, "Failed to generate Hash of Query Key");
				}

				if (port == myPort) {
					String value = allMessages.get(N - 1).get(selection);
					if (value == null) {
						Object object = queryMessages.get(selection);
						if (object == null) {
							object = new Object();
							queryMessages.put(selection, object);
						}
						synchronized (object) {
							try {
								object.wait();
							} catch (InterruptedException e) {
								Log.e(TAG, "Query wait failed for Key");
							}
						}
						value = allMessages.get(N - 1).get(selection);
					}
					cursor.addRow(new String[] {selection, value});
				} else {
					map = new ConcurrentHashMap<String, String>();
					map.put(selection, "");
					ClientThread clientThread = new ClientThread(myPort, port, QUERY, N - 1, map);
					Thread thread = new Thread(clientThread);
					thread.start();

					synchronized (localMessages) {
						try {
							localMessages.wait();
						} catch (InterruptedException e) {
							Log.e(TAG, "InterruptedException in Query Block");
						}
					}
					for(String key : localMessages.keySet()){
						cursor.addRow(new String[] {key, localMessages.get(key)});
					}
				}
			}
			return cursor;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	@SuppressWarnings("resource")
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
/*http://www.sanfoundry.com/java-program-implement-circular-doubly-linked-list/
* Creation of circular linked list*/

class CircularLinkedList {
	private Node linkedListHead = null;
	private int linkedListSize = 0;

	public Node getLinkedListHead() {
		return linkedListHead;
	}

	public void add(String nodeId, int portId) {
		Node node = new Node(nodeId, portId, null, null);

		if (this.getLinkedListHead()!=null){
			if (linkedListSize == 1){
				node.myPrevPort = linkedListHead;
				node.myNextPort = linkedListHead;
				linkedListHead.myPrevPort = node;
				linkedListHead.myNextPort = node;
				if (node.compareTo(linkedListHead) <= 0) {
					linkedListHead = node;
				}
			}
			else {
				Node curr = linkedListHead;
				for (int i = 0; i < this.getSize(); i = i + 1) {
					if (curr.myNextPort == linkedListHead) {
						node.myPrevPort = curr;
						node.myNextPort = linkedListHead;
						curr.myNextPort = node;
						linkedListHead.myPrevPort = node;
						break;
					} else if (node.compareTo(curr) <= 0){
						node.myPrevPort = curr.myPrevPort;
						node.myNextPort = curr;
						curr.myPrevPort.myNextPort = node;
						curr.myPrevPort = node;
						if (curr == linkedListHead) linkedListHead = node;
						break;
					}
					else {
						curr = curr.myNextPort;
					}
				}
			}
			linkedListSize = linkedListSize + 1;
		} else {
			node.myPrevPort = node;
			node.myNextPort = node;
			linkedListHead = node;
			linkedListSize = linkedListSize + 1;
		}
	}

	public int getSize() {
		return linkedListSize;
	}

	public int getPort(String hashKey) {
		return getNode(hashKey).myPort;
	}

	public int getQueryPort(String hashKey) {
		Node node = getNode(hashKey);
		for (int i = 0; i < SimpleDynamoProvider.N-1; i = i + 1) {
			node = node.myNextPort;
		}
		return node.myPort;
	}

	public List<Integer> getAllNodes(){
		List<Integer> list = new ArrayList<Integer>();
		Node curr = linkedListHead;
		for (int i = 0; i < linkedListSize; i = i + 1) {
			list.add(curr.myPort);
			curr = curr.myNextPort;
		}
		return list;
	}

	public int getNextNode(int port) {
		Node node = getNode(port);
		if (node != null) {
			return node.myNextPort.myPort;
		}
		return 0;
	}

	public int getPrevNode(int port) {
		Node node = getNode(port);
		if (node != null) {
			return node.myPrevPort.myPort;
		}
		return 0;
	}

	private Node getNode(String hashKey) {
		Node curr = linkedListHead;
		for (int i = 0; i < linkedListSize; i++) {
			if (hashKey.compareTo(curr.messageId) <= 0) {
				return curr;
			}
			curr = curr.myNextPort;
		}
		return curr;
	}

	private Node getNode(int port) {
		Node curr = linkedListHead;
		for (int i = 0; i < linkedListSize; i++) {
			if (curr.myPort == port) return curr;
			curr = curr.myNextPort;
		}
		return null;
	}

	class Node implements Comparable<Node> {
		private String messageId;
		private int myPort;
		private Node myPrevPort;
		private Node myNextPort;

		Node(String id, int port, Node prev, Node next) {
			messageId = id;
			myPort = port;
			myPrevPort = prev;
			myNextPort = next;
		}

		@Override
		public int compareTo(Node another) {
			return this.messageId.compareTo(another.messageId);
		}
	}
}