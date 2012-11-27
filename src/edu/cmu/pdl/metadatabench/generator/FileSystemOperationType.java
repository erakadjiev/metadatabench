package edu.cmu.pdl.metadatabench.generator;

public enum FileSystemOperationType {
	CREATE ("create"),
	DELETE ("delete"),
	LIST_STATUS ("listStatus"),
	MKDIRS ("mkdirs"),
	OPEN ("open"),
	RENAME ("rename");
	
	private final String name;
	
	private FileSystemOperationType(String name) {
		this.name = name;
	}
	
	public String getName(){
		return name;
	}
	
}
