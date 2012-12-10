package edu.cmu.pdl.metadatabench.generator;

import java.util.Random;

public class BarabasiAlbertCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private Random randomParent;

	public BarabasiAlbertCreationStrategy(INamespaceMapDAO dao){
		this(dao, "");
	}
	
	public BarabasiAlbertCreationStrategy(INamespaceMapDAO dao, String workingDirectory){
		super(dao, workingDirectory);
		randomId = new Random();
		randomParent = new Random();
	}
	
	public String selectDirectory(){
		int id = randomId.nextInt(numberOfDirs-1) + 2;
		String dirPath = dao.getDir(id);
		boolean parent = randomParent.nextBoolean();
		if(parent){
			int slashIdx = dirPath.lastIndexOf(PATH_SEPARATOR);
			dirPath = dirPath.substring(0, slashIdx);
		}
		return dirPath;
	}
	
}
