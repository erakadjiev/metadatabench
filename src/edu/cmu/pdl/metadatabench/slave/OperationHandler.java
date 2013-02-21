package edu.cmu.pdl.metadatabench.slave;

import edu.cmu.pdl.metadatabench.cluster.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.SimpleOperation;

public class OperationHandler {

	private static char PATH_SEPARATOR = '/';
	
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
				if(op instanceof CreateOperation){
					create(targetId, ((CreateOperation)op).getId(), ((CreateOperation)op).getName());
				} else {
					create(targetId);
				}
				break;
			case MKDIRS:
				if(op instanceof CreateOperation){
					mkdir(targetId, ((CreateOperation)op).getParentsParent(), ((CreateOperation)op).getId(), ((CreateOperation)op).getName());
				} else {
					mkdir(targetId);
				}
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
			case MOVE_FILE:
				moveFile(targetId);
				break;
		}
	}
	
	private void create(long id) {
		throw new UnsupportedOperationException("Create file operation cannot be handled.");
	}
	
	private void create(long parentId, long id, String name) {
		String parentPath = dao.getDir(parentId);
		while(parentPath == null){
			parentPath = dao.getDir(parentId);
		}
		String path = parentPath + name;
		dao.createFile(id, path);
		executor.create(path);
	}
	
	private void mkdir(long id) {
		String path = dao.getDir(id);
		executor.mkdir(path);
	}
	
	private void mkdir(long parentId, boolean parentsParent, long id, String name) {
		String parentPath = dao.getDir(parentId);
		while(parentPath == null){
			parentPath = dao.getDir(parentId);
		}
		if(parentsParent){
			int slashIdx = parentPath.lastIndexOf(PATH_SEPARATOR);
			parentPath = parentPath.substring(0, slashIdx);
		}
		String path = parentPath + name;
		dao.createDir(id, path);
		executor.mkdir(path);
	}

	private void deleteFile(long id) {
		throw new UnsupportedOperationException("Delete file operation cannot be handled.");
	}

	private void listStatusFile(long id) {
		String path = dao.getFile(id);
		executor.listStatusFile(path);
	}
	
	private void listStatusDir(long id) {
		String path = dao.getDir(id);
		executor.listStatusDir(path);
	}

	private void openFile(long id) {
		String path = dao.getFile(id);
		executor.open(path);
	}

	private void renameFile(long id) {
		throw new UnsupportedOperationException("Rename file operation cannot be handled.");
	}
	
	private void moveFile(long id) {
		throw new UnsupportedOperationException("Move file operation cannot be handled.");
	}
	
}
