package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.slave.Slave;

/**
 * A command to execute an operation on a directory or file.
 * The {@link FileSystemOperationType} parameter specifies the type of operation and target id 
 * identifies the element on which it has to be executed.
 * This class is used for operations that do not need extra parameters. E.g. delete, ls, open or even 
 * mkdir and create if the path has already been stored in the distributed map.
 * 
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class SimpleOperation implements Runnable, Serializable {

	private FileSystemOperationType type;
	private long targetId;
	
	/**
	 * @param type The type of operation to be executed
	 * @param targetId Identifies the element on which the operation has to be executed
	 */
	public SimpleOperation(FileSystemOperationType type, long targetId){
		this.type = type;
		this.targetId = targetId;
	}

	/**
	 * Gets the type of operation, e.g. create, mkdir, delete file, etc.
	 * 
	 * @return The type of operation
	 */
	public FileSystemOperationType getType() {
		return type;
	}

	/**
	 * Gets the id of the directory or file targeted by this operation
	 * 
	 * @return The id of the directory or file targeted by this operation
	 */
	public long getTargetId() {
		return targetId;
	}

	@Override
	public void run() {
		Slave.getOperationHandler().handleOperation(this);
	}
	
}
