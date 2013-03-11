package edu.cmu.pdl.metadatabench.master.progress;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes progress reports sent by slaves, monitors overall progress, provides possibility for the master to 
 * wait until all the operations have been executed.
 * 
 * @author emil.rakadjiev
 *
 */
public class ProgressMonitor {

	/** The number of operation executed by each slave */
	private static Map<Integer,Long> operationsDonePerNode = new ConcurrentHashMap<Integer,Long>();
	/** The total number of operations that have been executed */
	private static long operationsDoneTotal;
	/** The total number of operations that need to be executed */
	private static long operationsNeeded;
	private static CountDownLatch latch = new CountDownLatch(1);
	
	private static final Logger log = LoggerFactory.getLogger(ProgressMonitor.class);
	
	// TODO: implement timeout
	/**
	 * Blocks until all operations have been executed.
	 * 
	 * @param numberOfOperations The number of operations that need to be executed
	 * @throws InterruptedException If the current thread has been interrupted while waiting 
	 */
	public static void awaitOperationCompletion(long numberOfOperations) throws InterruptedException{
		if(sumOfOperations() < numberOfOperations){
			operationsNeeded = numberOfOperations;
			latch.await();
		}
	}
	
	/**
	 * Report how many operations has a given node executed.
	 * 
	 * @param nodeId The id of the node which sends the report
	 * @param operationsDone The number of operations executed by the node
	 */
	public static synchronized void reportCompletedOperations(int nodeId, long operationsDone){
		if(addOperations(nodeId, operationsDone)){
			int opsSum = sumOfOperations();
			operationsDoneTotal = opsSum;
			log.info("{} operations done", opsSum);
			if(opsSum == operationsNeeded){
				latch.countDown();
			}
		} else {
			log.debug("Received old progress report from node {}, ignoring it.", nodeId);
		}
	}
	
	/**
	 * Records how many operations a node has done and reported.
	 * If the progress report is old, it is ignored.
	 * 
	 * @param nodeId The id of the node which sends the report
	 * @param operationsDone The number of operations executed by the node
	 * @return True if the reported operations were recorded, false if the progress report was old and was thus ignored
	 */
	private static boolean addOperations(int nodeId, long operationsDone){
		Long opsOld = operationsDonePerNode.get(nodeId);
		/*
		 * Due to the asynchronous communication, an older progress report may be received after a newer one. 
		 * In this case, the newer value must not be overwritten.
		 */
		if((opsOld == null) || (operationsDone > opsOld)){
			operationsDonePerNode.put(nodeId, operationsDone);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Gets the total number of operations executed so far
	 * 
	 * @return The total number of operations executed so far
	 */
	public static long getOperationsDone(){
		return operationsDoneTotal;
	}
	
	/**
	 * Sums up the number of operations done by all nodes
	 * 
	 * @return The sum of operations done by all nodes
	 */
	private static int sumOfOperations(){
		int ops = 0;
		Collection<Long> values = operationsDonePerNode.values();
		for(long value : values){
			ops += value;
		}
		return ops;
	}
	
	/**
	 * Resets the progress data
	 */
	public static void reset(){
		operationsDonePerNode = new ConcurrentHashMap<Integer,Long>();
		operationsDoneTotal = 0;
		operationsNeeded = 0;
		latch = new CountDownLatch(1);
	}

}
