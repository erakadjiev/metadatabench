package edu.cmu.pdl.metadatabench.master;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ProgressBarrier {

	private static Map<Integer,Integer> operationsDonePerNode = new HashMap<Integer,Integer>();
	private static int operationsNeeded;
	private static CountDownLatch latch = new CountDownLatch(1);
	
	public static void awaitOperationCompletion(int numberOfOperations) throws InterruptedException{
		operationsNeeded = numberOfOperations;
		latch.await();
	}
	
	public static synchronized void reportCompletedOperations(int nodeId, int operationsDone){
		operationsDonePerNode.put(nodeId, operationsDone);
		int opsSum = sumOfOperations();
		System.out.println(opsSum + " operations done");
//		if(opsSum == operationsNeeded){
		if(opsSum == operationsNeeded){
			latch.countDown();
		}
	}
	
	private static int sumOfOperations(){
		int ops = 0;
		Collection<Integer> values = operationsDonePerNode.values();
		for(int value : values){
			ops += value;
		}
		return ops;
	}
	
	public static void reset(){
		operationsDonePerNode = new ConcurrentHashMap<Integer,Integer>();
		operationsNeeded = 0;
		latch = new CountDownLatch(1);
	}

}
