package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.slave.progress.Progress;
import edu.cmu.pdl.metadatabench.slave.progress.ProgressReporter;

@SuppressWarnings("serial")
public class ProgressReset implements Runnable, Serializable {

	@Override
	public void run() {
		Progress.reset();
		ProgressReporter.reset();
	}

}
