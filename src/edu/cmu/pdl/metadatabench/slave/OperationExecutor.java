package edu.cmu.pdl.metadatabench.slave;

import java.io.IOException;
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
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.create(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void delete(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.delete(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatus(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.listStatus(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void mkdir(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.mkdir(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void open(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.open(path);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void rename(final String fromPath, final String toPath){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.rename(fromPath, toPath);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					Progress.reportCompletedOperation(Thread.currentThread().getId());
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void shutdown(){
		threadPool.shutdown();
	}
	
}
