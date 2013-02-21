package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.SimpleOperation;


public abstract class AbstractFileCreationStrategy {

	protected static final char PATH_SEPARATOR = '/';
	protected static final String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	protected static final FileSystemOperationType CREATE_TYPE = FileSystemOperationType.CREATE; 
	
	protected long numberOfDirs;
	protected IOperationDispatcher dispatcher;
	
	public AbstractFileCreationStrategy(IOperationDispatcher dispatcher, long numberOfDirs){
		this.numberOfDirs = numberOfDirs;
		this.dispatcher = dispatcher;
	}
	
	abstract public long selectParentDirectory();
	
	public void createNextFile(int i){
		long parentId = selectParentDirectory();
		String name = FILE_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(CREATE_TYPE, parentId, i, name);
		dispatcher.dispatch(op);
	}
	
}
