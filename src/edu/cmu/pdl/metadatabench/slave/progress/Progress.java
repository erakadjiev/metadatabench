package edu.cmu.pdl.metadatabench.slave.progress;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the progress of the node, that is, the number of executed operations.
 * @author emil.rakadjiev
 *
 */
public class Progress {

	private static AtomicLong ops = new AtomicLong();
	private static Logger log = LoggerFactory.getLogger(Progress.class);
	
	/**
	 * Reports one completed operation
	 */
	public static void reportCompletedOperation(){
		ops.incrementAndGet();
	}
	
	/**
	 * Gets the number of operations executed so far
	 * @return The number of operations executed so far
	 */
	public static long getOperationsDone(){
		return ops.get();
	}
	
	/**
	 * Resets the progress data
	 */
	public static void reset(){
		log.info("All operations done");
		ops.set(0L);
	}
	
}
