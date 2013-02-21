package edu.cmu.pdl.metadatabench.cluster;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.slave.Progress;
import edu.cmu.pdl.metadatabench.slave.ProgressReporter;

@SuppressWarnings("serial")
public class ProgressReset implements Runnable, Serializable {

	@Override
	public void run() {
		Progress.reset();
		ProgressReporter.reset();
	}

}
