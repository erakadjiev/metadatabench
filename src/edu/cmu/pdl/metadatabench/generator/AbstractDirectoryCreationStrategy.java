package edu.cmu.pdl.metadatabench.generator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	protected static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	
	protected ExecutorService threadPool;
	
	private String workingDirectory;
	protected int numberOfDirs;
	protected INamespaceMapDAO dao;
	
	public AbstractDirectoryCreationStrategy(INamespaceMapDAO dao, String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith("/")){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		this.threadPool = Executors.newFixedThreadPool(100); // TODO param
		numberOfDirs = 0;
		this.dao = dao;
	}
	
	abstract public void createNextDirectory();
	
	public void createRoot(){
		numberOfDirs++;
		String rootPath = workingDirectory + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, rootPath);
		numberOfDirs++;
		String firstDirPath = rootPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, firstDirPath);
	}
	
}
