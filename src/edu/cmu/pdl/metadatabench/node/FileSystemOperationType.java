package edu.cmu.pdl.metadatabench.node;

public enum FileSystemOperationType {
	CREATE ("create"),
	MKDIRS ("mkdirs"),
	DELETE_FILE ("deleteFile"),
	LIST_STATUS_FILE ("listStatusFile"),
	LIST_STATUS_DIR ("listStatusDir"),
	OPEN_FILE ("openFile"),
	RENAME_FILE ("renameFile");
	
	private final String name;
	
	private FileSystemOperationType(String name) {
		this.name = name;
	}
	
	public String getName(){
		return name;
	}
	
}
