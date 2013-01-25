package edu.cmu.pdl.metadatabench.generator;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;

public class UniformCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private int masters;
	
	public UniformCreationStrategy(INamespaceMapDAO dao, int masters){
		this(dao, "", masters);
	}
	
	public UniformCreationStrategy(INamespaceMapDAO dao, String workingDirectory, int masters) {
		super(dao, workingDirectory);
		randomId = new Random();
		this.masters = masters;
	}

	@Override
	public long selectDirectory(int i) {
		int from = i;
		if(i > (masters)){
			from = i - ((i-1) % masters);
		}
		int key = randomId.nextInt(from-1) + 1;
		return (long) key;
	}

}
