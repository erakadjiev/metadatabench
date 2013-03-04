package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;


public abstract class AbstractFileCreationStrategy {

	protected static final char PATH_SEPARATOR = Config.getPathSeparator();
	protected static final String FILE_NAME_PREFIX = PATH_SEPARATOR + Config.getFileNamePrefix();
	protected static final FileSystemOperationType CREATE_TYPE = FileSystemOperationType.CREATE; 
	
	protected long numberOfDirs;
	protected IDispatcher dispatcher;
	
	public AbstractFileCreationStrategy(IDispatcher dispatcher, long numberOfDirs){
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
