package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.slave.Slave;

/**
 * A message notifying the slaves that the generation has been finished and it's time to shut down.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class ProgressFinished implements Runnable, Serializable {

	@Override
	public void run() {
		Slave.shutdown();
	}

}
