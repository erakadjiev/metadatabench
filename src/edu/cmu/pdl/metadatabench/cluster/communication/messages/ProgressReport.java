package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.master.progress.ProgressBarrier;

@SuppressWarnings("serial")
public class ProgressReport implements Runnable, Serializable {

	private int nodeId;
	private long operationsDone;
	
	public ProgressReport(int nodeId, long operationsDone) {
		this.nodeId = nodeId;
		this.operationsDone = operationsDone;
	}
	
	public int getNodeId() {
		return nodeId;
	}

	public long getOperationsDone() {
		return operationsDone;
	}

	@Override
	public void run() {
		ProgressBarrier.reportCompletedOperations(nodeId, operationsDone);
	}

}
