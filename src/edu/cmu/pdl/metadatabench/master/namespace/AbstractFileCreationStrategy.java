package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;


public abstract class AbstractFileCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	
	protected long numberOfFiles;
	protected long numberOfDirs;
	protected INamespaceMapDAO dao;
	
	public AbstractFileCreationStrategy(INamespaceMapDAO dao, long numberOfDirs){
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = 0;
		this.dao = dao;
	}
	
	abstract public String selectDirectory();
	
	public void createNextFile(){
		String parentPath = selectDirectory();
		numberOfFiles++;
		String name = parentPath + FILE_NAME_PREFIX + numberOfFiles;
		dao.createFile(numberOfFiles, name);
	}
	
}
