package edu.cmu.pdl.metadatabench.cluster;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

public class HazelcastDispatcher implements IOperationDispatcher {

	private static ExecutorService executorService;
	private static Member master;
	private static Set<Member> slaves;
	
	public HazelcastDispatcher(HazelcastInstance hazelcast) {
		executorService = hazelcast.getExecutorService();
	}

	@Override
	public void dispatch(SimpleOperation operation) {
		FutureTask<Long> task = new DistributedTask<Long>(operation, operation.getTargetId());
		executorService.execute(task);
	}

	@Override
	public void dispatch(ProgressReport report) {
		if(master == null){
			master = HazelcastCluster.getInstance().getMaster();
		}
		executorService.execute(new DistributedTask<Boolean>(report, true, master));
	}

	@Override
	public void dispatch(ProgressReset reset) {
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		System.out.println(slaves.size());
		executorService.execute(new DistributedTask<Boolean>(reset, true, slaves));
	}

	@Override
	public void dispatch(ProgressFinished finish) {
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		executorService.execute(new DistributedTask<Boolean>(finish, true, slaves));
	}

}
