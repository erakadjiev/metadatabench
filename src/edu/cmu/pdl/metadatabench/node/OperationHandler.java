package edu.cmu.pdl.metadatabench.node;

public class OperationHandler {

	private OperationExecutor executor;
	private INamespaceMapReader mapReader;
	
	public OperationHandler(OperationExecutor executor, INamespaceMapReader mapReader){
		this.executor = executor;
		this.mapReader = mapReader;
	};
	
	public void handleOperation(FileSystemOperationType type, long targetId){
		//TODO: extract into abstract class
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
	
	private void create(long id) {
		throw new UnsupportedOperationException("Create file operation cannot be handled.");
	}
	
	private void mkdir(long id) {
		throw new UnsupportedOperationException("Mkdir operation cannot be handled.");
	}

	private void deleteFile(long id) {
		throw new UnsupportedOperationException("Delete file operation cannot be handled.");
	}

	private void listStatusFile(long id) {
		String path = mapReader.getFile(id);
		executor.listStatus(path);
	}
	
	private void listStatusDir(long id) {
		String path = mapReader.getDir(id);
		executor.listStatus(path);
	}

	private void openFile(long id) {
		String path = mapReader.getFile(id);
		executor.open(path);
	}

	private void renameFile(long id) {
		throw new UnsupportedOperationException("Rename file operation cannot be handled.");
	}
	
}
