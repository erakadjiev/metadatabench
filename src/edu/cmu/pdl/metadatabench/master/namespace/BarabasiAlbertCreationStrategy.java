package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;

public class BarabasiAlbertCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private Random randomParent;
	private int masters;

	public BarabasiAlbertCreationStrategy(INamespaceMapDAO dao, IOperationDispatcher dispatcher, int masters){
		this(dao, dispatcher, "", masters);
	}
	
	public BarabasiAlbertCreationStrategy(INamespaceMapDAO dao, IOperationDispatcher dispatcher, String workingDirectory, int masters){
		super(dao, dispatcher, workingDirectory);
		randomId = new Random();
		randomParent = new Random();
		this.masters = masters;
	}
	
	public long selectDirectory(int i){
//		long dirs = numberOfDirs.get();
//		int id = randomId.nextInt((int)dirs - 1) + 2;
		int from = i;
		if(i > (masters)){
			from = i - ((i-1) % masters);
		}
		int key = randomId.nextInt(from-1) + 1;
		String dirPath = dao.getDir(key);
//		System.out.println("key missing: " + key);
		while(dirPath == null){
			dirPath = dao.getDir(key);
		}
//		System.out.println("key found: " + key);
		boolean parent = randomParent.nextBoolean();
		if(parent){
			int slashIdx = 0;
			slashIdx = dirPath.lastIndexOf(PATH_SEPARATOR);
			dirPath = dirPath.substring(0, slashIdx);
		}
//		return dirPath;
		return 0;
	}
	
}
