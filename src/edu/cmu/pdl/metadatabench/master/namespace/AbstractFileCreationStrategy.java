package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

/**
 * Provides basic functionality for file generation. Called by {@link edu.cmu.pdl.metadatabench.master.namespace.NamespaceGenerator}.
 * In each iteration, a parent directory is selected for the new file and a create command is dispatched to a slave.
 * 
 * @author emil.rakadjiev
 *
 */
public abstract class AbstractFileCreationStrategy {

	protected static final char PATH_SEPARATOR = Config.getPathSeparator();
	protected static final String FILE_NAME_PREFIX = PATH_SEPARATOR + Config.getFileNamePrefix();
	protected static final FileSystemOperationType CREATE_TYPE = FileSystemOperationType.CREATE; 
	
	protected long numberOfDirs;
	protected IDispatcher dispatcher;
	
	/**
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 * @param numberOfDirs The number of existing directories
	 */
	public AbstractFileCreationStrategy(IDispatcher dispatcher, long numberOfDirs){
		this.numberOfDirs = numberOfDirs;
		this.dispatcher = dispatcher;
	}
	
	/**
	 * Selects an existing directory as the parent for a new file
	 * 
	 * @return The id of the selected parent directory
	 */
	abstract public long selectParentDirectory();
	
	/**
	 * Creates a new file by selecting a parent directory, constructing and dispatching a create command.
	 * 
	 * @param i The sequence number of the new file
	 */
	public void createNextFile(int i){
		long parentId = selectParentDirectory();
		String name = FILE_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(CREATE_TYPE, parentId, i, name);
		dispatcher.dispatch(op);
	}
	
}
