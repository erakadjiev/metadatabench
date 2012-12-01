package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.Future;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class DirectoryEntryListener extends AbstractEntryListener implements EntryListener<Integer, String> {
	
	private CallableOperationFactory callableFactory;
	
	public DirectoryEntryListener(int creatorThreads) {
		super(creatorThreads);
		callableFactory = new CallableOperationFactory(operationExecutor);
	}

	@Override
	public void entryAdded(final EntryEvent<Integer, String> arg0) {
		Future<Long> result = threadPool.submit(callableFactory.makeCreateCallable(arg0.getValue()));
	}

	@Override
	public void entryEvicted(EntryEvent<Integer, String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void entryRemoved(EntryEvent<Integer, String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void entryUpdated(EntryEvent<Integer, String> arg0) {
		// TODO Auto-generated method stub
		
	}

}
