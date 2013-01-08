package edu.cmu.pdl.metadatabench.generator;

import java.util.Random;

public class UniformCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	
	public UniformCreationStrategy(INamespaceMapDAO dao){
		this(dao, "");
	}
	
	public UniformCreationStrategy(INamespaceMapDAO dao, String workingDirectory) {
		super(dao, workingDirectory);
		randomId = new Random();
	}

	@Override
	public long selectDirectoryId() {
		int id = randomId.nextInt(numberOfDirs-1) + 2;
		return id;
	}

}
