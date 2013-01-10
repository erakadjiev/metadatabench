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
	
	public void createNextDirectory(){
		final int id = randomId.nextInt(numberOfDirs-1) + 2;
		final boolean parent = randomParent.nextBoolean();
		
		threadPool.submit(new DirCreationRunnable(numberOfDirs++, id, parent));
	}
	
	private class DirCreationRunnable implements Runnable {

		private long dirNumber;
		private int id;
		private boolean parent;
		
		public DirCreationRunnable(long dirNumber, int id, boolean parent){
			this.dirNumber = dirNumber;
			this.id = id;
			this.parent = parent;
		}
		
		@Override
		public void run() {
			String dirPath = dao.getDir(id);
			if(parent){
				int slashIdx = dirPath.lastIndexOf(PATH_SEPARATOR);
				dirPath = dirPath.substring(0, slashIdx);
			}
			String name = dirPath + DIR_NAME_PREFIX + dirNumber;
			dao.createDir(dirNumber, name);
		}
	}
	
}
