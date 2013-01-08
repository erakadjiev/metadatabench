package edu.cmu.pdl.metadatabench.node;

@SuppressWarnings("serial")
public class CreateOperation extends SimpleOperation {

	private long id;
	private String name;
	
	public CreateOperation(FileSystemOperationType type, long parentId, long id, String name) {
		super(type, parentId);
		this.id = id;
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public long getId() {
		return id;
	}

}
