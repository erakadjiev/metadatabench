package edu.cmu.pdl.metadatabench.cluster;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.master.ProgressBarrier;

@SuppressWarnings("serial")
public class ProgressReport implements Runnable, Serializable {

	private int nodeId;
	private int operationsDone;
	
	public ProgressReport(int nodeId, int operationsDone) {
		this.nodeId = nodeId;
		this.operationsDone = operationsDone;
	}
	
	public int getNodeId() {
		return nodeId;
	}

	public int getOperationsDone() {
		return operationsDone;
	}

	@Override
	public void run() {
		ProgressBarrier.reportCompletedOperations(nodeId, operationsDone);
	}

}
