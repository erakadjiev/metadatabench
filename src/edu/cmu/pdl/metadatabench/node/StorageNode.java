package edu.cmu.pdl.metadatabench.node;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class StorageNode{

	private static final int THREADS = 100; // TODO: param
	private static final HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);
	private static final OperationExecutor executor = new OperationExecutor(new HDFSClient(), THREADS);
	private static final OperationHandler handler = new OperationHandler(executor, new HazelcastMapReader(hazelcast));
	
	public static void main(String[] args) {
		IMap<Integer,String> dirMap = hazelcast.getMap("directories");
		dirMap.addLocalEntryListener(new DirectoryEntryListener(executor));

		IMap<Integer,String> fileMap = hazelcast.getMap("files");
		fileMap.addLocalEntryListener(new FileEntryListener(executor));
	}
	
	public static OperationHandler getOperationHandler(){
		return handler;
	}

}
