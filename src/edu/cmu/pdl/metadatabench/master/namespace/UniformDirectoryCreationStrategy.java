package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;

public class UniformDirectoryCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private int masters;
	
	public UniformDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher, int masters){
		this(dao, dispatcher, "", masters);
	}
	
	public UniformDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher, String workingDirectory, int masters) {
		super(dao, dispatcher, workingDirectory);
		randomId = new Random();
		this.masters = masters;
	}

	@Override
	public long selectParentDirectory(int i) {
		int from = i;
		if(i > (masters)){
			from = i - ((i-1) % masters);
		}
		int key = randomId.nextInt(from-1) + 1;
		return (long) key;
	}

}
