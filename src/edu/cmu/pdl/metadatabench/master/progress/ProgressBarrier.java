package edu.cmu.pdl.metadatabench.master.progress;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressBarrier {

	private static Map<Integer,Long> operationsDonePerNode = new ConcurrentHashMap<Integer,Long>();
	private static long operationsDoneTotal;
	private static long operationsNeeded;
	private static CountDownLatch latch = new CountDownLatch(1);
	
	private static final Logger log = LoggerFactory.getLogger(ProgressBarrier.class);
	
	// TODO: implement timeout
	public static void awaitOperationCompletion(long numberOfOperations) throws InterruptedException{
		if(sumOfOperations() < numberOfOperations){
			operationsNeeded = numberOfOperations;
			latch.await();
		}
	}
	
	public static synchronized void reportCompletedOperations(int nodeId, long operationsDone){
		operationsDonePerNode.put(nodeId, operationsDone);
		int opsSum = sumOfOperations();
		operationsDoneTotal = opsSum;
		log.info("{} operations done", opsSum);
		if(opsSum == operationsNeeded){
			latch.countDown();
		}
	}
	
	public static long getOperationsDone(){
		return operationsDoneTotal;
	}
	
	private static int sumOfOperations(){
		int ops = 0;
		Collection<Long> values = operationsDonePerNode.values();
		for(long value : values){
			ops += value;
		}
		return ops;
	}
	
	public static void reset(){
		operationsDonePerNode = new ConcurrentHashMap<Integer,Long>();
		operationsDoneTotal = 0;
		operationsNeeded = 0;
		latch = new CountDownLatch(1);
	}

}
