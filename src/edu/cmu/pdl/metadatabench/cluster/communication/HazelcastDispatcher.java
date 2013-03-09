package edu.cmu.pdl.metadatabench.cluster.communication;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;

import edu.cmu.pdl.metadatabench.cluster.HazelcastCluster;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsCollect;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressFinished;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReport;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

/**
 * Dispatches messages and commands to nodes in a Hazelcast cluster.
 * 
 * @author emil.rakadjiev
 *
 */
public class HazelcastDispatcher implements IDispatcher {

	/** The distributed executor service */
	private static ExecutorService executorService;
	private static Member master;
	private static Set<Member> slaves;
	
	/**
	 * @param hazelcast The relevant Hazelcast instance
	 */
	public HazelcastDispatcher(HazelcastInstance hazelcast) {
		executorService = hazelcast.getExecutorService();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * From master to one slave. Asynchronous.
	 * The operation is sent to the owner of the key referenced by the operation. This way the owner 
	 * can access the element in its local memory.
	 */
	@Override
	public void dispatch(SimpleOperation operation) {
		executorService.execute(new DistributedTask<Boolean>(operation, true, operation.getTargetId()));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * From slave to master. Asynchronous.
	 */
	@Override
	public void dispatch(ProgressReport report) {
		if(master == null){
			master = HazelcastCluster.getInstance().getMaster();
		}
		executorService.execute(new DistributedTask<Boolean>(report, true, master));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * From master to all slaves. Asynchronous.
	 */
	@Override
	public void dispatch(ProgressReset reset) {
		executorService.execute(new DistributedTask<Boolean>(reset, true, getSlaves()));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * From master to all slaves. Asynchronous.
	 */
	@Override
	public void dispatch(ProgressFinished finish) {
		executorService.execute(new DistributedTask<Boolean>(finish, true, getSlaves()));
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * From master to all slaves. Asynchronous.
	 */
	@Override
	public void dispatch(MeasurementsReset reset) {
		executorService.execute(new DistributedTask<Boolean>(reset, true, getSlaves()));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * From master to all slaves. Synchronous.
	 */
	@Override
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception{
		MultiTask<MeasurementDataForNode> task = new MultiTask<MeasurementDataForNode>(collectMeasurement, getSlaves()); 
		executorService.execute(task);
		return task.get();
	}
	
	/**
	 * Gets the set of slaves. Caches it on the first access.
	 * @return
	 */
	private Set<Member> getSlaves(){
		if(slaves == null){
			slaves = HazelcastCluster.getInstance().getSlaves();
		}
		return slaves;
	}

}
