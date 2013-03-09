package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.slave.progress.Progress;
import edu.cmu.pdl.metadatabench.slave.progress.ProgressReporter;

/**
 * A message notifying the slaves that a generation step (directory creation, file creation or workload) 
 * has been completed and they should reset their progress.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class ProgressReset implements Runnable, Serializable {

	@Override
	public void run() {
		Progress.reset();
		ProgressReporter.reset();
	}

}
