package edu.cmu.pdl.metadatabench.node;

public abstract class AbstractEntryListener {

	protected OperationExecutor executor;
	
	public AbstractEntryListener(OperationExecutor executor){
		this.executor = executor;
	}
	
}
