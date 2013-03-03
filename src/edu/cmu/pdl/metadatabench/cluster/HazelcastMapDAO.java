package edu.cmu.pdl.metadatabench.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastMapDAO implements INamespaceMapDAO {

	private IMap<Long,String> dirMap;
	private IMap<Long,String> fileMap;
	
	private static final String DIR_MAP_NAME = "directories";
	private static final String FILE_MAP_NAME = "files";
	
	public HazelcastMapDAO(HazelcastInstance hazelcast){
		dirMap = hazelcast.getMap(DIR_MAP_NAME);
		fileMap = hazelcast.getMap(FILE_MAP_NAME);
	}
	
	@Override
	public void createDir(long id, String path) {
		dirMap.putAsync(id, path);
	}

	@Override
	public String getDir(long id) {
		return dirMap.get(id);
	}
	
	@Override
	public void deleteDir(long id) {
		dirMap.remove(id);
	}

	@Override
	public long getNumberOfDirs() {
		return dirMap.size();
	}

	@Override
	public void createFile(long id, String path) {
		fileMap.putAsync(id, path);
	}

	@Override
	public String getFile(long id) {
		return fileMap.get(id);
	}

	@Override
	public void deleteFile(long id) {
		fileMap.removeAsync(id);
	}
	
	@Override
	public void renameFile(long id, String pathNew){
		fileMap.replace(id, pathNew);
	}

	@Override
	public long getNumberOfFiles() {
		return fileMap.size();
	}

}
