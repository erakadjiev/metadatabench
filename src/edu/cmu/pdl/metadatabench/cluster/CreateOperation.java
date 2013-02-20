package edu.cmu.pdl.metadatabench.cluster;

@SuppressWarnings("serial")
public class CreateOperation extends SimpleOperation {

	private boolean parentsParent;
	private long id;
	private String name;
	
	public CreateOperation(FileSystemOperationType type, long parentId, long id, String name) {
		super(type, parentId);
		this.parentsParent = false;
		this.id = id;
		this.name = name;
	}
	
	public CreateOperation(FileSystemOperationType type, long parentId, boolean parentsParent, long id, String name) {
		super(type, parentId);
		this.parentsParent = parentsParent;
		this.id = id;
		this.name = name;
	}
	
	public boolean getParentsParent(){
		return parentsParent;
	}

	public String getName() {
		return name;
	}

	public long getId() {
		return id;
	}

}
