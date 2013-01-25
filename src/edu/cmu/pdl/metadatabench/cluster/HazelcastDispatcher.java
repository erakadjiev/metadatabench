package edu.cmu.pdl.metadatabench.cluster;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.partition.PartitionService;

public class HazelcastDispatcher implements IOperationDispatcher {

	private static ExecutorService executorService;
	private static PartitionService partitionService;
	
	public HazelcastDispatcher(HazelcastInstance hazelcast) {
		executorService = hazelcast.getExecutorService();
		partitionService = hazelcast.getPartitionService();
	}

	@Override
	public void dispatch(SimpleOperation operation) {
		Member owner = partitionService.getPartition(operation.getTargetId()).getOwner();
		FutureTask<Long> task = new DistributedTask<Long>(operation, owner);
		executorService.execute(task);
	}

}
