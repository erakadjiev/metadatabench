package edu.cmu.pdl.metadatabench.cluster;

import java.io.Serializable;
import java.util.concurrent.Callable;

import edu.cmu.pdl.metadatabench.slave.Slave;

@SuppressWarnings("serial")
public class SimpleOperation implements Callable<Long>, Serializable {

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
	public Long call() throws Exception {
		Slave.getOperationHandler().handleOperation(this);
		return null;
	}
	
}
