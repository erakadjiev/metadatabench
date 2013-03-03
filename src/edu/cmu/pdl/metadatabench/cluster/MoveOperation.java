package edu.cmu.pdl.metadatabench.cluster;

@SuppressWarnings("serial")
public class MoveOperation extends SimpleOperation {

	private long parentIdNew;
	
	public MoveOperation(FileSystemOperationType type, long id, long parentIdNew) {
		super(type, id);
		this.parentIdNew = parentIdNew;
	}
	
	public long getParentIdNew() {
		return parentIdNew;
	}

}
