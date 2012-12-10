package edu.cmu.pdl.metadatabench.generator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.partition.PartitionService;

public class HazelcastMapDAO implements INamespaceMapDAO, IOperationDispatcher {

	private HazelcastInstance hazelcast;
	private ExecutorService executorService;
	private PartitionService partitionService;
	private IMap<Long,String> dirMap;
	private IMap<Long,String> fileMap;
	
	public HazelcastMapDAO(){
		hazelcast = Hazelcast.newHazelcastInstance(null);
		executorService = hazelcast.getExecutorService();
		partitionService = hazelcast.getPartitionService();
		dirMap = hazelcast.getMap("directories");
		fileMap = hazelcast.getMap("files");
	}
	
	@Override
	public void createDir(long id, String path) {
		dirMap.put(id, path);
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
		fileMap.put(id, path);
	}

	@Override
	public String getFile(long id) {
		return fileMap.get(id);
	}

	@Override
	public void deleteFile(long id) {
		fileMap.remove(id);
	}

	@Override
	public long getNumberOfFiles() {
		return fileMap.size();
	}

	// TODO: extract to separate class 
	@Override
	public void dispatch(SimpleOperation operation) {
		Member owner = partitionService.getPartition(operation.getTargetId()).getOwner();
		FutureTask<Long> task = new DistributedTask<Long>(operation, owner);
		executorService.execute(task);
	}

}
