package edu.cmu.pdl.metadatabench.node;

public class OperationHandler {

	private OperationExecutor executor;
	private INamespaceMapDAO dao;
	
	public OperationHandler(OperationExecutor executor, INamespaceMapDAO dao){
		this.executor = executor;
		this.dao = dao;
	};
	
	public void handleOperation(SimpleOperation op){
		//TODO: extract into abstract class
		FileSystemOperationType type = op.getType();
		long targetId = op.getTargetId();
		switch(type){
			case CREATE:
				create(targetId);
				break;
			case MKDIRS:
				mkdir(targetId);
				break;
			case DELETE_FILE:
				deleteFile(targetId);
				break;
			case LIST_STATUS_FILE:
				listStatusFile(targetId);
				break;
			case LIST_STATUS_DIR:
				listStatusDir(targetId);
				break;
			case OPEN_FILE:
				openFile(targetId);
				break;
			case RENAME_FILE:
				renameFile(targetId);
				break;
		}
	}
	
	public void handleOperation(CreateOperation op){
		FileSystemOperationType type = op.getType();
		long targetId = op.getTargetId();
		CreateOperation createOp = (CreateOperation) op;
		switch(type){
			case MKDIRS:
				mkdir(targetId, createOp.getId(), createOp.getName());
				break;
			default:
				break;
		}
	}
	
	private void create(long id) {
		throw new UnsupportedOperationException("Create file operation cannot be handled.");
	}
	
	private void mkdir(long id) {
//		throw new UnsupportedOperationException("Mkdir operation cannot be handled.");
		String path = dao.getDir(id);
		executor.mkdir(path);
	}
	
	private void mkdir(long parentId, long id, String name) {
//		throw new UnsupportedOperationException("Mkdir operation cannot be handled.");
		String parentPath = dao.getDir(parentId);
		String path = parentPath + name;
		dao.createDir(id, path);
		executor.mkdir(path);
	}

	private void deleteFile(long id) {
		throw new UnsupportedOperationException("Delete file operation cannot be handled.");
	}

	private void listStatusFile(long id) {
		String path = dao.getFile(id);
		executor.listStatus(path);
	}
	
	private void listStatusDir(long id) {
		String path = dao.getDir(id);
		executor.listStatus(path);
	}

	private void openFile(long id) {
		String path = dao.getFile(id);
		executor.open(path);
	}

	private void renameFile(long id) {
		throw new UnsupportedOperationException("Rename file operation cannot be handled.");
	}
	
}
