package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;

/**
 * A directory creation strategy that selects a parent directory from the existing set of directories 
 * using a uniform distribution
 * 
 * @author emil.rakadjiev
 *
 */
public class UniformDirectoryCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	
	/**
	 * @param dao The DAO to use for accessing the namespace map
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 */
	public UniformDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher) {
		super(dao, dispatcher);
		randomId = new Random();
	}

	/**
	 * Selects a parent directory from the set of existing directories using a uniform distribution.
	 * It is assumed that all directories with a sequence number lower than the sequence number i of 
	 * the directory to be created already exist. Thus, the id of the parent is a random number between 
	 * 1 and i-1
	 */
	@Override
	public long selectParentDirectory(int i) {
		// select a random number between 0 and i-2 and adjust the interval to [1,i-1] by adding 1
		int key = randomId.nextInt(i-1) + 1;
		return (long) key;
	}

}
