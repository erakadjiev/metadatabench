package edu.cmu.pdl.metadatabench.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MoveOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

/**
 * Handles operations received from the master according to their type. Reads or modifies the necessary data in 
 * the distributed namespace map and forwards the operations for execution. 
 * 
 * @author emil.rakadjiev
 *
 */
public class OperationHandler {

	private static char PATH_SEPARATOR = Config.getPathSeparator();
	private static String RENAME_SUFFIX = Config.getWorkloadRenameSuffix();
	
	private OperationExecutor executor;
	private INamespaceMapDAO dao;
	
	private Logger log;
	
	/**
	 * @param executor The operation executor
	 * @param dao The DAO used to access the distributed namespace
	 */
	public OperationHandler(OperationExecutor executor, INamespaceMapDAO dao){
		this.executor = executor;
		this.dao = dao;
		this.log = LoggerFactory.getLogger(OperationHandler.class);
	};
	
	/**
	 * Handles an operation according to its type
	 * @param op The operation to handle
	 */
	public void handleOperation(SimpleOperation op){
		FileSystemOperationType type = op.getType();
		long targetId = op.getTargetId();
		switch(type){
			case CREATE:
				if(op instanceof CreateOperation){
					create(targetId, ((CreateOperation)op).getId(), ((CreateOperation)op).getName());
				} else {
					log.warn("Error: {} operation type has to have a CreateOperation object", FileSystemOperationType.CREATE.getName());
				}
				break;
			case MKDIRS:
				if(op instanceof CreateOperation){
					mkdir(targetId, ((CreateOperation)op).getParentsParent(), ((CreateOperation)op).getId(), ((CreateOperation)op).getName());
				} else {
					// create a directory in the file system which is already stored in the distributed namespace map
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
					log.warn("Error: {} operation type has to have a MoveOperation object", FileSystemOperationType.MOVE_FILE.getName());
				}
				break;
			default:
				log.warn("Error: Unknown operation type received");
		}
	}
	
	/**
	 * Handles a create operation. Reads the path of the parent directory from the distributed namespace map, 
	 * constructs the path of the new file, updates the distributed namespace map and forwards the operation 
	 * for execution on the underlying file system. 
	 * 
	 * @param parentId The id of the parent directory
	 * @param id The id of the file to be created
	 * @param name The name of the file to be created
	 */
	private void create(long parentId, long id, String name) {
		String parentPath = dao.getDir(parentId);
		// TODO: timeout if parent directory not found
		/*
		 * It can happen that the parent directory has not yet been created, because this operation "overtook" the 
		 * corresponding mkdir operation
		 */
		while(parentPath == null){
			parentPath = dao.getDir(parentId);
		}
		String path = parentPath + name;
		dao.createFile(id, path);
		executor.create(path);
	}
	
	/**
	 * Handles a mkdir operation for a directory that has already been inserted into the distributed namespace map 
	 * (but not yet executed on the namespace map).
	 * @param id The id of the directory to create
	 */
	private void mkdir(long id){
		String path = dao.getDir(id);
		// TODO: timeout if parent directory not found
		/*
		 * It can happen that directory has not yet been inserted into the distributed namespace map.
		 */
		while(path == null){
			path = dao.getDir(id);
		}
		executor.mkdir(path);
	}
	
	/**
	 * Handles a mkdir operation. Reads the path of the parent directory (or its parent) from the distributed 
	 * namespace map, constructs the path of the new directory, updates the distributed namespace map and forwards 
	 * the operation for execution on the underlying file system. 
	 * 
	 * @param parentId The id of the parent directory
	 * @param parentsParent Whether the parent directories parent should be the parent of the new directory  
	 * @param id The id of the directory to be created
	 * @param name The name of the directory to be created
	 */
	private void mkdir(long parentId, boolean parentsParent, long id, String name) {
		String parentPath = dao.getDir(parentId);
		// TODO: timeout if parent directory not found
		/*
		 * It can happen that the parent directory has not yet been created, because this operation "overtook" the 
		 * corresponding mkdir operation
		 */
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

	/**
	 * Handles a delete operation. Reads the path of the file from the distributed namespace map, updates the 
	 * distributed namespace map and forwards the operation for execution on the underlying file system. 
	 * 
	 * @param id The id of the file to be deleted
	 */
	private void deleteFile(long id) {
		String path = dao.getFile(id);
		executor.delete(path);
		dao.deleteFile(id);
	}

	/**
	 * Handles an ls file operation. Reads the path of the file from the distributed namespace map 
	 * and forwards the operation for execution on the underlying file system. 
	 * 
	 * @param id The id of the file
	 */
	private void listStatusFile(long id) {
		String path = dao.getFile(id);
		executor.listStatusFile(path);
	}
	
	/**
	 * Handles an ls dir operation. Reads the path of the directory from the distributed namespace map 
	 * and forwards the operation for execution on the underlying file system. 
	 * 
	 * @param id The id of the directory
	 */
	private void listStatusDir(long id) {
		String path = dao.getDir(id);
		executor.listStatusDir(path);
	}

	/**
	 * Handles an open operation. Reads the path of the file from the distributed namespace map 
	 * and forwards the operation for execution on the underlying file system. 
	 * 
	 * @param id The id of the file to open
	 */
	private void openFile(long id) {
		String path = dao.getFile(id);
		executor.open(path);
	}

	/**
	 * Handles a rename operation. Reads the path of the file from the distributed namespace map, 
	 * constructs the new path, updates the distributed namespace map and forwards the operation for 
	 * execution on the underlying file system. 
	 * 
	 * @param id The id of the file to rename
	 */
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
	
	/**
	 * Handles a move operation. Reads the path of the file and the new parent directory from the 
	 * distributed namespace map, constructs the new path, updates the distributed namespace map and 
	 * forwards the operation for execution on the underlying file system. 
	 * 
	 * @param id The id of the file to move
	 * @param parentIdNew The id of the new parent directory
	 */
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
	
	/**
	 * Appends or increments a rename suffix at the end of the file name.
	 * When an element is renamed, a suffix including a rename count is appended to or incremented at the end 
	 * of its name. For example file20 -> file20.r1 or file20.r69 -> file20.r70.
	 * @param path The path of the file to rename
	 * @return The updated path
	 */
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
