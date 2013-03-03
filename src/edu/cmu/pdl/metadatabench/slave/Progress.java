package edu.cmu.pdl.metadatabench.slave;

import java.util.concurrent.atomic.AtomicLong;

public class Progress {

	private static AtomicLong ops = new AtomicLong();
	
	public static void reportCompletedOperation(){
		ops.incrementAndGet();
	}
	
	public static long getOperationsDone(){
		return ops.get();
	}
	
	public static void reset(){
		System.out.println("All operations done.");
		ops.set(0L);
	}
	
}
