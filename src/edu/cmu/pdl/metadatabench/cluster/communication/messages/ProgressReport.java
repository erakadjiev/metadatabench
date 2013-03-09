package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.master.progress.ProgressMonitor;

/**
 * A message sent by a slave to notify the master how many operations it has completed so far.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class ProgressReport implements Runnable, Serializable {

	private int nodeId;
	private long operationsDone;
	
	/**
	 * @param nodeId The id of the slave sending the report
	 * @param operationsDone The number of operations that the slave has done so far
	 */
	public ProgressReport(int nodeId, long operationsDone) {
		this.nodeId = nodeId;
		this.operationsDone = operationsDone;
	}
	
	/**
	 * Gets the id of the slave sending the report
	 * @return The id of the slave sending the report
	 */
	public int getNodeId() {
		return nodeId;
	}

	/**
	 * Gets the number of operations that the slave has done so far
	 * @return The number of operations that the slave has done so far
	 */
	public long getOperationsDone() {
		return operationsDone;
	}

	@Override
	public void run() {
		ProgressMonitor.reportCompletedOperations(nodeId, operationsDone);
	}

}
