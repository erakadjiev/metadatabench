package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.hazelcast.core.ICountDownLatch;

public class OperationExecutor {

	private final IFileSystemClient client;
	private final ExecutorService threadPool;
	private final ICountDownLatch latch;
	
	public OperationExecutor(IFileSystemClient client, int threadCount){
		this.client = client;
		this.threadPool = Executors.newFixedThreadPool(threadCount);
		this.latch = Slave.getHazelcastInstance().getCountDownLatch("latch");
	}
	
	public void create(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.create(path);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void delete(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.delete(path);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatus(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.listStatus(path);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void mkdir(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.mkdir(path);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void open(final String path){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.open(path);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
	public void rename(final String fromPath, final String toPath){
		Callable<Long> op = new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				long runtime = 0;
//				long runtime = client.rename(fromPath, toPath);
//				latch.countDown();
				return runtime;
			}
		};
		threadPool.submit(op);
	}
	
}
