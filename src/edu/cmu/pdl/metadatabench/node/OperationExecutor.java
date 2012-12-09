package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OperationExecutor {

	private final IFileSystemClient client;
	private final ExecutorService threadPool;
	
	public OperationExecutor(IFileSystemClient client, int threadCount){
		this.client = client;
		this.threadPool = Executors.newFixedThreadPool(threadCount);
	}
	
	public void create(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.create(path);
			}
		};
		threadPool.submit(op);
	}
	
	public void delete(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.delete(path);
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatus(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.listStatus(path);
			}
		};
		threadPool.submit(op);
	}
	
	public void mkdir(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.mkdir(path);
			}
		};
		threadPool.submit(op);
	}
	
	public void open(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.open(path);
			}
		};
		threadPool.submit(op);
	}
	
	public void rename(final String fromPath, final String toPath){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				return client.rename(fromPath, toPath);
			}
		};
		threadPool.submit(op);
	}
	
}
