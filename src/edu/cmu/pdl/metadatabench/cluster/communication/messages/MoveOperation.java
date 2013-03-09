package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

/**
 * A command to move a directory or file to a new parent folder.
 * As opposed to {@link SimpleOperation}, this class is used for move operations that 
 * require an extra parameter. Besides the operation type, and the element id, there is also a parameter 
 * for the new parent's id. 
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class MoveOperation extends SimpleOperation {

	private long parentIdNew;
	
	/**
	 * @param type The operation type
	 * @param id The id of the element to be moved
	 * @param parentIdNew The id of the new parent directory
	 */
	public MoveOperation(FileSystemOperationType type, long id, long parentIdNew) {
		super(type, id);
		this.parentIdNew = parentIdNew;
	}
	
	/**
	 * Gets the id of the new parent directory
	 * @return The id of the new parent directory
	 */
	public long getParentIdNew() {
		return parentIdNew;
	}

}
