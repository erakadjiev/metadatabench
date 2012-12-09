package edu.cmu.pdl.metadatabench.generator;


public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	
	private String workingDirectory;
	protected int numberOfDirs;
	protected INamespaceMapEntryDAO dao;
	
	public AbstractDirectoryCreationStrategy(INamespaceMapEntryDAO dao, String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith("/")){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		numberOfDirs = 0;
		this.dao = dao;
	}
	
	abstract public String selectDirectory();
	
	public void createNextDirectory(){
		String parentPath = selectDirectory();
		numberOfDirs++;
		String name = parentPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, name);
	}
	
	public void createRoot(){
		numberOfDirs++;
		String rootPath = workingDirectory + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, rootPath);
		numberOfDirs++;
		String firstDirPath = rootPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, firstDirPath);
	}
	
	public void testPrint(){
		System.out.println(dao.getNumberOfDirs());
		System.out.println(dao.getDir(1));
		System.out.println(dao.getDir(dao.getNumberOfDirs()/2));
	}
	
}
