package edu.cmu.pdl.metadatabench.slave;

import edu.cmu.pdl.metadatabench.cluster.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.MoveOperation;
import edu.cmu.pdl.metadatabench.cluster.SimpleOperation;

public class OperationHandler {

	private static char PATH_SEPARATOR = '/';
	private static String RENAME_SUFFIX = ".r";
	
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
					System.out.println("\"CREATE\" operation type has to have a CreateOperation object");;
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
				if(op instanceof MoveOperation){
					moveFile(targetId, ((MoveOperation)op).getParentIdNew());
				} else {
					System.out.println("\"MOVE\" operation type has to have a MoveOperation object");
				}
				break;
			default:
				System.out.println("Unknown operation");
		}
	}
	
	private void create(long parentId, long id, String name) {
		String parentPath = dao.getDir(parentId);
		// TODO: timeout if parent directory not found
		while(parentPath == null){
			parentPath = dao.getDir(parentId);
		}
		String path = parentPath + name;
		dao.createFile(id, path);
		executor.create(path);
	}
	
	private void mkdir(long id){
		String path = dao.getDir(id);
		while(path == null){
			path = dao.getDir(id);
		}
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
		String path = dao.getFile(id);
		executor.delete(path);
		dao.deleteFile(id);
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
		String path = dao.getFile(id);
		String pathNew = path;
		try{
			pathNew = incrementRenameCounterExtension(path);
			dao.renameFile(id, pathNew);
		} catch (NullPointerException e){
		} finally {
			executor.rename(path, pathNew);
		}
	}
	
	private void moveFile(long id, long parentIdNew) {
		String path = dao.getFile(id);
		String parentPathNew = dao.getDir(parentIdNew);
		String pathNew = path;
		try{
			int slashIdx = path.lastIndexOf(PATH_SEPARATOR);
			String fileName = path.substring(slashIdx);
			pathNew = parentPathNew + fileName;
			if(pathNew.equals(path)){
				pathNew = incrementRenameCounterExtension(pathNew);
			}
			dao.renameFile(id, pathNew);
		} catch(NullPointerException e){
		} finally {
			executor.move(path, pathNew);
		}
	}
	
	private String incrementRenameCounterExtension(String path){
		int slashIdx = path.lastIndexOf(PATH_SEPARATOR);
		String fileName = path.substring(slashIdx+1);
		if(fileName.contains(RENAME_SUFFIX)){
			int suffixIdx = path.lastIndexOf(RENAME_SUFFIX);
			String pathWithoutSuffix = path.substring(0, suffixIdx);
			int renameCounter = Integer.parseInt(path.substring(suffixIdx+2));
			renameCounter++;
			return pathWithoutSuffix + RENAME_SUFFIX + renameCounter;
		} else {
			return path + RENAME_SUFFIX + "1";
		}
	}
	
}
