package edu.cmu.pdl.metadatabench.master;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ProgressBarrier {

	private static Map<Integer,Long> operationsDonePerNode = new ConcurrentHashMap<Integer,Long>();
	private static long operationsNeeded;
	private static CountDownLatch latch = new CountDownLatch(1);
	
	public static void awaitOperationCompletion(long numberOfOperations) throws InterruptedException{
		if(sumOfOperations() < numberOfOperations){
			operationsNeeded = numberOfOperations;
			latch.await();
		}
	}
	
	public static synchronized void reportCompletedOperations(int nodeId, long operationsDone){
		operationsDonePerNode.put(nodeId, operationsDone);
		int opsSum = sumOfOperations();
		System.out.println(opsSum + " operations done");
		if(opsSum == operationsNeeded){
			latch.countDown();
		}
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
		operationsNeeded = 0;
		latch = new CountDownLatch(1);
	}

}
