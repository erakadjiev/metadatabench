package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;
import java.util.concurrent.Callable;

import edu.cmu.pdl.metadatabench.slave.Slave;

/**
 * A message notifying a slave that the previously generated namespace has to be deleted.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class NamespaceDelete implements Callable<Integer>, Serializable {

	/** The path of the work directory of the benchmark */
	private String workDir;
	
	public NamespaceDelete(String workDir){
		this.workDir = workDir;
	}
	
	@Override
	public Integer call() throws Exception {
		return Slave.getOperationExecutor().getFileSystemClient().delete(workDir);
	}

}
