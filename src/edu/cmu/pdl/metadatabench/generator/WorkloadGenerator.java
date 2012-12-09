package edu.cmu.pdl.metadatabench.generator;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGenerator {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	
	private static int NUM_OPS = 100000; //TODO: param
	public static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = new HashMap<FileSystemOperationType,Double>();
	static{
		OPERATION_PROBABILITIES.put(FileSystemOperationType.CREATE, 0.2);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS, 0.4);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.OPEN, 0.4);
	}
	
	private long numberOfDirs;
	private long numberOfFiles;
	private INamespaceMapEntryDAO dao;
	private IOperationDispatcher dispatcher;
	private OperationTypeSelector operationTypeSelector;
	private DirectoryAndFileSelector randomSelector;
	
	public WorkloadGenerator(INamespaceMapEntryDAO dao, long numberOfDirs, long numberOfFiles){
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dao = dao;
		this.dispatcher = (IOperationDispatcher) dao;
		operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		randomSelector = new DirectoryAndFileSelector();
	}
	
	public void generate(){
		for(int i=0; i<NUM_OPS; i++){
			FileSystemOperationType operation = operationTypeSelector.getRandomOperationType();
			switch(operation){
			case CREATE:
				create();
				break;
			case DELETE:
				delete();
				break;
			case LIST_STATUS:
				listStatus();
				break;
			case MKDIRS:
				mkdir();
				break;
			case OPEN:
				open();
				break;
			case RENAME:
				rename();
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

	private void delete() {
		long id = randomSelector.getRandomFile(numberOfFiles);
		dao.deleteFile(id);
		// TODO: track deleted files
	}

	private void listStatus() {
		long id = randomSelector.getRandomDirectory(numberOfDirs);
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.LIST_STATUS, id);
		dispatcher.dispatch(op);
	}

	private void mkdir() {
		long parentId = randomSelector.getRandomDirectory(numberOfDirs);
		String parentPath = dao.getDir(parentId);
		numberOfDirs++;
		String path = parentPath + DIR_NAME_PREFIX + numberOfDirs;
		dao.createDir(numberOfDirs, path);
	}

	private void open() {
		long id = randomSelector.getRandomDirectory(numberOfDirs);
		SimpleOperation op = new SimpleOperation(FileSystemOperationType.OPEN, id);
		dispatcher.dispatch(op);
	}

	private void rename() {
		long id = randomSelector.getRandomFile(numberOfFiles);
		// TODO: what should be the new name?
	}
	
}
