package edu.cmu.pdl.metadatabench.slave;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Progress {

	private static Map<Long,Integer> operationsDonePerThread = new ConcurrentHashMap<Long,Integer>();
	
	public static void reportCompletedOperation(long threadId){
		int ops = operationsDonePerThread.containsKey(threadId) ? operationsDonePerThread.get(threadId) : 0;
		operationsDonePerThread.put(threadId, ++ops);
	}
	
	public static int getOperationsDone(long threadId){
		return operationsDonePerThread.get(threadId); 
	}
	
	public static int getOperationsDone(){
		int ops = 0;
		Collection<Integer> values = operationsDonePerThread.values();
		for(int value : values){
			ops += value;
		}
		return ops;
	}
	
	public static void reset(){
		System.out.println("All operations done.");
		operationsDonePerThread = new HashMap<Long,Integer>();
	}
	
}
