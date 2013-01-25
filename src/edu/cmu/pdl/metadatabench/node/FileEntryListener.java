package edu.cmu.pdl.metadatabench.node;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class FileEntryListener extends AbstractEntryListener implements EntryListener<Integer, String> {
	
	public FileEntryListener(OperationExecutor executor) {
		super(executor);
	}

	@Override
	public void entryAdded(final EntryEvent<Integer, String> arg0) {
		executor.create(arg0.getValue());
	}

	@Override
	public void entryEvicted(EntryEvent<Integer, String> arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void entryRemoved(EntryEvent<Integer, String> arg0) {
		executor.delete(arg0.getValue());
	}

	@Override
	public void entryUpdated(EntryEvent<Integer, String> arg0) {
		executor.rename(arg0.getOldValue(), arg0.getValue());
	}

}