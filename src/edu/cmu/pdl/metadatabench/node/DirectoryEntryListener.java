package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class DirectoryEntryListener extends AbstractEntryListener implements EntryListener<Integer, String> {
	
	public DirectoryEntryListener(int creatorThreads) {
		super(creatorThreads);
	}

	@Override
	public void entryAdded(final EntryEvent<Integer, String> arg0) {
		Future<Long> result = threadPool.submit(new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return hdfsClient.create(arg0.getValue());
			}
			
		});
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
