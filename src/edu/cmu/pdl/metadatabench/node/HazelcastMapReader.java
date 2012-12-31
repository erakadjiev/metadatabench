package edu.cmu.pdl.metadatabench.node;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastMapReader implements INamespaceMapReader {

	private HazelcastInstance hazelcast;
	private IMap<Long,String> dirMap;
	private IMap<Long,String> fileMap;
	
	public HazelcastMapReader(HazelcastInstance hazelcast){
		this.hazelcast = hazelcast;
		dirMap = this.hazelcast.getMap("directories");
		fileMap = this.hazelcast.getMap("files");
	}
	
	@Override
	public String getDir(long id) {
		return dirMap.get(id);
	}

	@Override
	public String getFile(long id) {
		return fileMap.get(id);
	}

}
