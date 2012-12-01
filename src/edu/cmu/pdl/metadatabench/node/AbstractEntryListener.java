package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class AbstractEntryListener {

	protected ExecutorService threadPool;
	protected IOperationExecutor operationExecutor;
	
	public AbstractEntryListener(int creatorThreads){
		threadPool = Executors.newFixedThreadPool(creatorThreads);
		operationExecutor = new HDFSOperationExecutor();
	}
	
}
