package edu.cmu.pdl.metadatabench.node;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class StorageNode{

	private static final int THREADS = 100; // TODO: param
	private static final HazelcastInstance hazelcast;
	private static final OperationExecutor executor;
	private static final OperationHandler handler;
	
	static{
		System.clearProperty("hazelcast.lite.member");
		Config config = new ClasspathXmlConfig("hazelcast-node.xml");
		hazelcast = Hazelcast.newHazelcastInstance(config);
		executor = new OperationExecutor(new HDFSClient(), THREADS);
		handler = new OperationHandler(executor, new HazelcastMapDAO(hazelcast));
	}
	
	public static void main(String[] args) {
//		IMap<Integer,String> dirMap = hazelcast.getMap("directories");
//		dirMap.addLocalEntryListener(new DirectoryEntryListener(executor));
//
//		IMap<Integer,String> fileMap = hazelcast.getMap("files");
//		fileMap.addLocalEntryListener(new FileEntryListener(executor));
	}
	
	public static OperationHandler getOperationHandler(){
		return handler;
	}
	
	public static HazelcastInstance getHazelcastInstance(){
		return hazelcast;
	}

}
