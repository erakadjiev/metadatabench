package edu.cmu.pdl.metadatabench.node;

import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class StorageNode{

	private static int creatorThreads = 100; // TODO: param
	
	public static void main(String[] args) {
		HazelcastInstance hci = null;
		Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
		if(instances.size() == 1){
			for (HazelcastInstance instance : instances){
				hci = instance;
			}
		} else {
			System.err.println("Less or more than 1 Hazelcast instances available");
		}
		IMap<Integer,String> dirMap = hci.getMap("directories");
		dirMap.addLocalEntryListener(new DirectoryEntryListener(creatorThreads));
	}

}
