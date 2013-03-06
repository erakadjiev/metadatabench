package edu.cmu.pdl.metadatabench.slave;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.measurement.Measurements;
import edu.cmu.pdl.metadatabench.slave.fs.IFileSystemClient;
import edu.cmu.pdl.metadatabench.slave.progress.Progress;

public class OperationExecutor {

	private static final String CREATE_NAME = FileSystemOperationType.CREATE.getName();
	private static final String DELETE_NAME = FileSystemOperationType.DELETE_FILE.getName();
	private static final String LIST_STATUS_FILE_NAME = FileSystemOperationType.LIST_STATUS_FILE.getName();
	private static final String LIST_STATUS_DIR_NAME = FileSystemOperationType.LIST_STATUS_DIR.getName();
	private static final String MKDIR_NAME = FileSystemOperationType.MKDIRS.getName();
	private static final String OPEN_NAME = FileSystemOperationType.OPEN_FILE.getName();
	private static final String RENAME_NAME = FileSystemOperationType.RENAME_FILE.getName();
	private static final String MOVE_NAME = FileSystemOperationType.MOVE_FILE.getName();
	
	private final IFileSystemClient client;
	private final ExecutorService threadPool;
	private final Measurements measurements;
	
	private Logger log;
	
	public OperationExecutor(IFileSystemClient client, int threadCount){
		this.client = client;
		this.threadPool = Executors.newFixedThreadPool(threadCount);
		this.measurements = Measurements.getMeasurements();
		this.log = LoggerFactory.getLogger(OperationExecutor.class);
	}
	
	public void create(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					int runtime = client.create(path);
					if(runtime > 10000){
						log.debug("File creation took too long: {}", path);
					}
					measurements.measure(CREATE_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(CREATE_NAME, e.getClass().getName());
					log.debug(CREATE_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
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
					int runtime = client.delete(path);
					measurements.measure(DELETE_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(DELETE_NAME, e.getClass().getName());
					log.debug(DELETE_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatusFile(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					int runtime = client.listStatus(path);
					measurements.measure(LIST_STATUS_FILE_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(LIST_STATUS_FILE_NAME, e.getClass().getName());
					log.debug(LIST_STATUS_FILE_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void listStatusDir(final String path){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					int runtime = client.listStatus(path);
					measurements.measure(LIST_STATUS_DIR_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(LIST_STATUS_DIR_NAME, e.getClass().getName());
					log.debug(LIST_STATUS_DIR_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
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
					int runtime = client.mkdir(path);
					measurements.measure(MKDIR_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(MKDIR_NAME, e.getClass().getName());
					log.debug(MKDIR_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
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
					int runtime = client.open(path);
					measurements.measure(OPEN_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(OPEN_NAME, e.getClass().getName());
					log.debug(OPEN_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
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
					int runtime = client.rename(fromPath, toPath);
					measurements.measure(RENAME_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(RENAME_NAME, e.getClass().getName());
					log.debug(RENAME_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void move(final String fromPath, final String toPath){
		Runnable op = new Runnable(){
			@Override
			public void run() {
				try {
					int runtime = client.move(fromPath, toPath);
					measurements.measure(MOVE_NAME, runtime);
				} catch (Exception e) {
					measurements.reportException(MOVE_NAME, e.getClass().getName());
					log.debug(MOVE_NAME + " operation cannot be executed", e);
				} finally {
					Progress.reportCompletedOperation();
				}
			}
		};
		threadPool.submit(op);
	}
	
	public void shutdown(){
		threadPool.shutdown();
	}
	
}
