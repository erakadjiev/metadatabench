package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.slave.Slave;

@SuppressWarnings("serial")
public class SimpleOperation implements Runnable, Serializable {

	private FileSystemOperationType type;
	private long targetId;
	
	public SimpleOperation(FileSystemOperationType type, long targetId){
		this.type = type;
		this.targetId = targetId;
	}

	public FileSystemOperationType getType() {
		return type;
	}

	public long getTargetId() {
		return targetId;
	}

	@Override
	public void run() {
		Slave.getOperationHandler().handleOperation(this);
	}
	
}
