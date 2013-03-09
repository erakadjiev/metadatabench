package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.master.namespace.BarabasiAlbertDirectoryCreationStrategy;

/**
 * A command to create a directory or file.
 * As opposed to {@link SimpleOperation}, this class is used for create or mkdir operations that 
 * require more parameters. Besides the operation type, there are parameters for the parent directory's id, 
 * the new element's id and its name (not path) and a flag whether the element should be created 
 * in the given parent directory or as a sibling of the parent directory (this flag is needed for the 
 * {@link BarabasiAlbertDirectoryCreationStrategy} Barabasi-Albert directory generation algorithm).
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class CreateOperation extends SimpleOperation {

	private boolean parentsParent;
	private long id;
	private String name;
	
	/**
	 * @param type The type of the operation (create or mkdir)
	 * @param parentId Id of the parent directory
	 * @param id The id of the new element
	 * @param name The name of the new element (not path)
	 */
	public CreateOperation(FileSystemOperationType type, long parentId, long id, String name) {
		super(type, parentId);
		this.parentsParent = false;
		this.id = id;
		this.name = name;
	}
	
	/**
	 * @param type The type of the operation (create or mkdir)
	 * @param parentId Id of the parent directory
	 * @param parentsParent a flag whether the element should be created in the given parent directory 
	 * or as a sibling of the parent directory, i.e. in the parent's parent. This flag is needed for the 
	 * {@link BarabasiAlbertDirectoryCreationStrategy} Barabasi-Albert directory generation algorithm.
	 * @param id The id of the new element
	 * @param name The name of the new element (not path)
	 */
	public CreateOperation(FileSystemOperationType type, long parentId, boolean parentsParent, long id, String name) {
		super(type, parentId);
		this.parentsParent = parentsParent;
		this.id = id;
		this.name = name;
	}
	
	/**
	 * Should the new element be created in the given parent or in the parent's parent?
	 * 
	 * @return True if the new element should be created in the given parent or in the parent's parent
	 */
	public boolean getParentsParent(){
		return parentsParent;
	}

	/**
	 * Gets the name of the new element (not the path)
	 * 
	 * @return The name of the new element (not the path)
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the id of the new element
	 * 
	 * @return The id of the new element
	 */
	public long getId() {
		return id;
	}

}
