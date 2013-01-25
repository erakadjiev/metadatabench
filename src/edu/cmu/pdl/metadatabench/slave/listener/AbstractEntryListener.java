package edu.cmu.pdl.metadatabench.slave.listener;

import edu.cmu.pdl.metadatabench.slave.OperationExecutor;

public abstract class AbstractEntryListener {

	protected OperationExecutor executor;
	
	public AbstractEntryListener(OperationExecutor executor){
		this.executor = executor;
	}
	
}
