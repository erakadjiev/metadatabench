package edu.cmu.pdl.metadatabench.generator;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGenerator {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	
	private int numberOfOperations;
	public static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = new HashMap<FileSystemOperationType,Double>();
	static{
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS_FILE, 0.2);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS_DIR, 0.4);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.OPEN_FILE, 0.3);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.DELETE_FILE, 0.3);
	}
	
	private long numberOfDirs;
	private long numberOfFiles;
	private INamespaceMapDAO dao;
	private IOperationDispatcher dispatcher;
	private OperationTypeSelector operationTypeSelector;
	private DirectoryAndFileSelector randomSelector;
	
	public WorkloadGenerator(INamespaceMapDAO dao, int numberOfOperations, long numberOfDirs, long numberOfFiles){
		this.numberOfOperations = numberOfOperations;
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dao = dao;
		this.dispatcher = (IOperationDispatcher) dao;
		operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		randomSelector = new DirectoryAndFileSelector(numberOfDirs, numberOfFiles);
	}
	
	public void generate(){
		for(int i=0; i<numberOfOperations; i++){
			FileSystemOperationType operation = operationTypeSelector.getRandomOperationType();
			switch(operation){
				case CREATE:
					create();
					break;
				case MKDIRS:
					mkdir();
					break;
				case DELETE_FILE:
					deleteFile();
					break;
				case LIST_STATUS_FILE:
					listStatusFile();
					break;
				case LIST_STATUS_DIR:
					listStatusDir();
					break;
				case OPEN_FILE:
					openFile();
					break;
				case RENAME_FILE:
					renameFile();
					break;
			}
		}
	}
	
	private void create() {
		long parentId = randomSelector.getRandomDirectory(numberOfDirs);
		String parentPath = dao.getDir(parentId);
		numberOfFiles++;
		String path = parentPath + FILE_NAME_PREFIX + numberOfFiles;
		dao.createFile(numberOfFiles, path);
	}
	
	private void mkdir() {
		long parentId = randomSelector.getRandomDirectory(numberOfDirs);
		String parentPath = dao.getDir(parentId);
		numberOfDirs++;
		String path = parentPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, path);
	}

	private void deleteFile() {
		long id = randomSelector.getRandomFile(numberOfFiles);
		dao.deleteFile(id);
		// TODO: track deleted files
	}

	private void listStatusFile() {
		long id = randomSelector.getRandomFile(numberOfFiles);
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.LIST_STATUS_FILE, id);
		dispatcher.dispatch(op);
	}
	
	private void listStatusDir() {
		long id = randomSelector.getRandomDirectory(numberOfDirs);
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.LIST_STATUS_DIR, id);
		dispatcher.dispatch(op);
	}

	private void openFile() {
		long id = randomSelector.getRandomFile(numberOfFiles);
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.OPEN_FILE, id);
		dispatcher.dispatch(op);
	}

	private void renameFile() {
//		long id = randomSelector.getRandomFile(numberOfFiles);
		// TODO: what should be the new name?
	}
	
}
