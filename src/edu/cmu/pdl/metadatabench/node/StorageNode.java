package edu.cmu.pdl.metadatabench.node;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class StorageNode{

	private static final int THREADS = 100; // TODO: param
	private static final OperationExecutor executor = new OperationExecutor(new HDFSClient(), THREADS);
	
	private StorageNode(){};
	
	public static void main(String[] args) {
		HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);
		
		IMap<Integer,String> dirMap = hazelcast.getMap("directories");
		dirMap.addLocalEntryListener(new DirectoryAndFileEntryListener(executor));

		IMap<Integer,String> fileMap = hazelcast.getMap("files");
		fileMap.addLocalEntryListener(new DirectoryAndFileEntryListener(executor));
	}
	
	public static OperationExecutor getOperationExecutor(){
		return executor;
	}

}
