package edu.cmu.pdl.metadatabench.master.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.common.Config;

public class Throttler {

	private static final int THROTTLE_AFTER_GENERATED_OPS = Config.getWorkloadThrottleAfterGeneratedOps();
	private static final int THROTTLE_CONTINUE_THRESHOLD = Config.getWorkloadThrottleContinueThreshold();
	private static final int THROTTLE_DURATION = Config.getWorkloadThrottleDuration();

	private static final Logger log = LoggerFactory.getLogger(Throttler.class);
	
	public static void throttle(int i){
		if((i % THROTTLE_AFTER_GENERATED_OPS) == 0){
			int waitForOps = i - THROTTLE_CONTINUE_THRESHOLD;
			while(waitForOps >= ProgressBarrier.getOperationsDone()){
				try {
					log.debug("Waiting for the so far generated operations to be executed.");
					log.debug("Going to sleep for {} ms", THROTTLE_DURATION);
					Thread.sleep(THROTTLE_DURATION);
				} catch (InterruptedException e) {
					log.warn("Thread was interrupted while sleeping", e);
				}
			}
		}
	}

}
