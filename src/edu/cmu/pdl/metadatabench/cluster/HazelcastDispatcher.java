package edu.cmu.pdl.metadatabench.cluster;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;

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
		executorService.execute(new DistributedTask<Boolean>(reset, true, getSlaves()));
	}

	@Override
	public void dispatch(ProgressFinished finish) {
		executorService.execute(new DistributedTask<Boolean>(finish, true, getSlaves()));
	}

	@Override
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception{
		MultiTask<MeasurementDataForNode> task = new MultiTask<MeasurementDataForNode>(collectMeasurement, getSlaves()); 
		executorService.execute(task);
		return task.get();
	}

	@Override
	public void dispatch(MeasurementsReset reset) {
		executorService.execute(new DistributedTask<Boolean>(reset, true, getSlaves()));
	}
	
	private Set<Member> getSlaves(){
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		return slaves;
	}

}
