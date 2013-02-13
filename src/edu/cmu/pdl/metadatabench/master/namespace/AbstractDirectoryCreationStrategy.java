package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.SimpleOperation;


public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	private static final FileSystemOperationType MKDIR_TYPE = FileSystemOperationType.MKDIRS; 
	
	private String workingDirectory;
	protected INamespaceMapDAO dao;
	protected IOperationDispatcher dispatcher;
	
	public AbstractDirectoryCreationStrategy(INamespaceMapDAO dao, IOperationDispatcher dispatcher, String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith("/")){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		this.dao = dao;
		this.dispatcher = dispatcher;
	}
	
	abstract public long selectDirectory(int i);
	
	public void createNextDirectory(int i){
		long parentId = selectDirectory(i);
//		long dirs = numberOfDirs.incrementAndGet();
//		String name = parentPath + DIR_NAME_PREFIX + dirs;
//		dao.createDir(dirs, name);
		String name = DIR_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(MKDIR_TYPE, parentId, i, name);
		dispatcher.dispatch(op);
	}
	
	public void createRoot(){
		String rootPath = workingDirectory + DIR_NAME_PREFIX + 1;
		dao.createDir(1, rootPath);
		SimpleOperation op = new SimpleOperation(MKDIR_TYPE, 1);
		dispatcher.dispatch(op);
	}
	
}
