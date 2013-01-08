package edu.cmu.pdl.metadatabench.generator;


public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	
	private String workingDirectory;
	protected int numberOfDirs;
	protected INamespaceMapDAO dao;
	
	public AbstractDirectoryCreationStrategy(INamespaceMapDAO dao, String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith("/")){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		numberOfDirs = 0;
		this.dao = dao;
	}
	
//	abstract public String selectDirectory();
	abstract public long selectDirectoryId();
	
	public void createNextDirectory(){
		long parentId = selectDirectoryId();
		numberOfDirs++;
		String name = DIR_NAME_PREFIX + numberOfDirs;
		dispatch(parentId, numberOfDirs, name);
	}
	
	public void createRoot(){
		numberOfDirs++;
		String rootPath = workingDirectory + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, rootPath);
		dispatch(numberOfDirs);
		numberOfDirs++;
		String firstDirPath = rootPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, firstDirPath);
		dispatch(numberOfDirs);
	}

	private void dispatch(long id){
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.MKDIRS, id);
		((IOperationDispatcher)dao).dispatch(op);
	}
	
	private void dispatch(long parentId, long id, String name){
		CreateOperation op = new CreateOperation(FileSystemOperationType.MKDIRS, parentId, id, name);
		((IOperationDispatcher)dao).dispatch(op);
	}
	
}
