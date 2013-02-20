package edu.cmu.pdl.metadatabench.cluster;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;

import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

public class HazelcastDispatcher implements IOperationDispatcher {

	private static ExecutorService executorService;
	private static Member master;
	private static Set<Member> slaves;
	
	public HazelcastDispatcher(HazelcastInstance hazelcast) {
		executorService = hazelcast.getExecutorService();
	}

	@Override
	public void dispatch(SimpleOperation operation) {
		executorService.execute(new DistributedTask<Long>(operation, operation.getTargetId()));
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
		executecOnSlaves(new DistributedTask<Boolean>(reset, true, slaves));
	}

	@Override
	public void dispatch(ProgressFinished finish) {
		executecOnSlaves(new DistributedTask<Boolean>(finish, true, slaves));
	}

	@Override
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception{
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		MultiTask<MeasurementDataForNode> task = new MultiTask<MeasurementDataForNode>(collectMeasurement, slaves); 
		executorService.execute(task);
		return task.get();
	}

	@Override
	public void dispatch(MeasurementsReset reset) {
		executecOnSlaves(new DistributedTask<Boolean>(reset, true, slaves));
	}
	
	private void executecOnSlaves(FutureTask<?> task){
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		executorService.execute(task);
	}

}
