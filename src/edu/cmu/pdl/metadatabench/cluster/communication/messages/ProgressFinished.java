package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.slave.Slave;

@SuppressWarnings("serial")
public class ProgressFinished implements Runnable, Serializable {

	@Override
	public void run() {
		Slave.shutdown();
	}

}
