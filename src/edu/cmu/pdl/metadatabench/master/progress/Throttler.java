package edu.cmu.pdl.metadatabench.master.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.common.Config;

/**
 * Provides throttling for the namespace and workload generator.
 * 
 * The generators generate and dispatch a possibly very large amount of operations in a loop. 
 * If the generators are faster than the slaves or the file system under test, then unexecuted operations 
 * could queue up and slow the down the benchmark. Thus, there is the option of throttling the generation 
 * and preventing such an overload of the distributed system.
 * It can be specified after how many generated operations should the throttler check whether the slaves 
 * could process a sufficient amount of the generated operations. For example, if the corresponding parameter is 
 * set to 100000, then after every 100000th generated operations, the master will check how many operations 
 * have been executed by the slaves and if needed, it will sleep until a given threshold has been reached.
 * 
 * @author emil.rakadjiev
 *
 */
public class Throttler {

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps() */
	private static final int THROTTLE_AFTER_GENERATED_OPS = Config.getWorkloadThrottleAfterGeneratedOps();
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() */
	private static final int THROTTLE_CONTINUE_THRESHOLD = Config.getWorkloadThrottleContinueThreshold();
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleDuration() */
	private static final int THROTTLE_DURATION = Config.getWorkloadThrottleDuration();

	private static final Logger log = LoggerFactory.getLogger(Throttler.class);
	
	/**
	 * If a predefined number of operations has been generated since the last check, checks how many operations 
	 * have the slaves processed in the meantime. If less than a given amount of operations have been executed, 
	 * then the throttler pauses and checks the progress regularly. When the needed number of operations have 
	 * been executed, then the generation can continue.
	 * 
	 * @param i The number of operations generated (generation loop iterator)
	 */
	public static void throttle(int i){
		if((i % THROTTLE_AFTER_GENERATED_OPS) == 0){
			int waitForOps = i - THROTTLE_CONTINUE_THRESHOLD;
			log.info("Pausing generation. Waiting for the so far generated operations to be executed.");
			while(waitForOps >= ProgressMonitor.getOperationsDone()){
				try {
					log.debug("Going to sleep for {} ms", THROTTLE_DURATION);
					Thread.sleep(THROTTLE_DURATION);
				} catch (InterruptedException e) {
					log.warn("Thread was interrupted while sleeping", e);
				}
			}
			log.info("Continuing generation.");
		}
	}

}
