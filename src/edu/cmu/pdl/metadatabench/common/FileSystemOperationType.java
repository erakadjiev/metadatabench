package edu.cmu.pdl.metadatabench.common;

/**
 * Types of file system metadata operations.
 * 
 * @author emil.rakadjiev
 *
 */
public enum FileSystemOperationType {
	CREATE ("create"),
	MKDIRS ("mkdirs"),
	DELETE_FILE ("deleteFile"),
	LIST_STATUS_FILE ("listStatusFile"),
	LIST_STATUS_DIR ("listStatusDir"),
	OPEN_FILE ("openFile"),
	RENAME_FILE ("renameFile"),
	MOVE_FILE ("moveFile");
	
	private final String name;
	
	private FileSystemOperationType(String name) {
		this.name = name;
	}
	
	/**
	 * Gets the name of the operation
	 * @return The name of the operation
	 */
	public String getName(){
		return name;
	}
	
}
