package edu.cmu.pdl.metadatabench.slave;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.pdl.metadatabench.slave.fs.IFileSystemClient;

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
				long runtime = client.create(path);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void delete(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = client.delete(path);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatus(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = client.listStatus(path);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void mkdir(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = client.mkdir(path);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void open(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = client.open(path);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void rename(final String fromPath, final String toPath){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = client.rename(fromPath, toPath);
				Progress.reportCompletedOperation(Thread.currentThread().getId());
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void shutdown(){
		threadPool.shutdown();
	}
	
}
