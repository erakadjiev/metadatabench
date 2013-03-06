package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;

public class UniformDirectoryCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	
	public UniformDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher) {
		super(dao, dispatcher);
		randomId = new Random();
	}

	@Override
	public long selectParentDirectory(int i) {
		int key = randomId.nextInt(i-1) + 1;
		return (long) key;
	}

}
