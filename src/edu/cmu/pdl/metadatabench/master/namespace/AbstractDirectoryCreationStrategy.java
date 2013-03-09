package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

/**
 * Provides basic functionality for directory generation. Called by {@link edu.cmu.pdl.metadatabench.master.namespace.NamespaceGenerator}.
 * First, a root directory must be created. In subsequent iterations, a parent directory is selected for the 
 * new directory and a mkdir command is dispatched to a slave.
 * 
 * @author emil.rakadjiev
 *
 */
public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = Config.getPathSeparator();
	protected static String DIR_NAME_PREFIX = PATH_SEPARATOR + Config.getDirNamePrefix();
	protected static final FileSystemOperationType MKDIR_TYPE = FileSystemOperationType.MKDIRS; 
	
	private String workDirectory;
	protected INamespaceMapDAO dao;
	protected IDispatcher dispatcher;
	
	/**
	 * @param dao The DAO to use for accessing the namespace map
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 */
	public AbstractDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher){
		this.workDirectory = Config.getWorkDir();
		while(this.workDirectory.endsWith(Character.toString(PATH_SEPARATOR))){
			this.workDirectory = this.workDirectory.substring(0, this.workDirectory.length() - 1);
		}
		this.dao = dao;
		this.dispatcher = dispatcher;
	}
	
	/**
	 * Selects an existing directory as the parent for a new directory
	 * 
	 * @param i The sequence number of the new directory
	 * @return The id of the selected parent directory
	 */
	abstract public long selectParentDirectory(int i);
	
	/**
	 * Creates a new directory by selecting a parent directory, constructing and dispatching a mkdir command.
	 * 
	 * @param i The sequence number of the new directory
	 */
	public void createNextDirectory(int i){
		long parentId = selectParentDirectory(i);
		String name = DIR_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(MKDIR_TYPE, parentId, i, name);
		dispatcher.dispatch(op);
	}
	
	/**
	 * Creates a root directory
	 */
	public void createRoot(){
		String rootPath = workDirectory + DIR_NAME_PREFIX + 1;
		dao.createDir(1, rootPath);
		SimpleOperation op = new SimpleOperation(MKDIR_TYPE, 1);
		dispatcher.dispatch(op);
	}
	
}
