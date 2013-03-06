package edu.cmu.pdl.metadatabench.slave.progress;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Progress {

	private static AtomicLong ops = new AtomicLong();
	private static Logger log = LoggerFactory.getLogger(Progress.class);
	
	public static void reportCompletedOperation(){
		ops.incrementAndGet();
	}
	
	public static long getOperationsDone(){
		return ops.get();
	}
	
	public static void reset(){
		log.info("All operations done");
		ops.set(0L);
	}
	
}
