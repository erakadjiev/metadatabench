package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;


public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = Config.getPathSeparator();
	protected static String DIR_NAME_PREFIX = PATH_SEPARATOR + Config.getDirNamePrefix();
	protected static final FileSystemOperationType MKDIR_TYPE = FileSystemOperationType.MKDIRS; 
	
	private String workingDirectory;
	protected INamespaceMapDAO dao;
	protected IDispatcher dispatcher;
	
	public AbstractDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher, String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith(Character.toString(PATH_SEPARATOR))){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		this.dao = dao;
		this.dispatcher = dispatcher;
	}
	
	abstract public long selectParentDirectory(int i);
	
	public void createNextDirectory(int i){
		long parentId = selectParentDirectory(i);
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
