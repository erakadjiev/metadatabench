package edu.cmu.pdl.metadatabench.slave;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.pdl.metadatabench.measurement.Measurements;

import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.slave.fs.IFileSystemClient;

public class OperationExecutor {

	private static final String CREATE_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String DELETE_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String LIST_STATUS_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String MKDIR_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String OPEN_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String RENAME_NAME = FileSystemOperationType.MKDIRS.getName();
	
	private static final int FAILURE_CODE = -1;
	
	private final IFileSystemClient client;
	private final ExecutorService threadPool;
	private final Measurements measurements;
	
	public OperationExecutor(IFileSystemClient client, int threadCount){
		this.client = client;
		this.threadPool = Executors.newFixedThreadPool(threadCount);
		this.measurements = Measurements.getMeasurements();
	}
	
	public void create(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					long runtime = client.create(path);
					measurements.measure(CREATE_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(CREATE_NAME, FAILURE_CODE);
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
					measurements.measure(DELETE_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(DELETE_NAME, FAILURE_CODE);
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
					measurements.measure(LIST_STATUS_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(LIST_STATUS_NAME, FAILURE_CODE);
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
					measurements.measure(MKDIR_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(MKDIR_NAME, FAILURE_CODE);
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
					measurements.measure(OPEN_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(OPEN_NAME, FAILURE_CODE);
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
					measurements.measure(RENAME_NAME, (int)runtime);
				} catch (IOException e) {
					e.printStackTrace();
					measurements.reportReturnCode(RENAME_NAME, FAILURE_CODE);
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
