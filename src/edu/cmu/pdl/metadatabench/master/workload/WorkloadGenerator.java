package edu.cmu.pdl.metadatabench.master.workload;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import edu.cmu.pdl.metadatabench.cluster.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.MoveOperation;
import edu.cmu.pdl.metadatabench.cluster.SimpleOperation;
import edu.cmu.pdl.metadatabench.master.IdCache;

public class WorkloadGenerator {

	//TODO: params
	private static final int DELETED_FILE_SET_MAX_SIZE = 100000;
	private static final int ACCESSED_ELEMENT_CACHE_MAX_SIZE = 110000;
	private static final long ACCESSED_ELEMENT_CACHE_TTL = 5000;
	private static final int THROTTLE_AFTER_ITERATIONS = 100000;
	private static final int THROTTLE_MILLIS = 15000;
	
	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	
	private int numberOfOperations;
	public static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = new HashMap<FileSystemOperationType,Double>();
	static{
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS_FILE, 0.4);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS_DIR, 0.05);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.OPEN_FILE, 0.35);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.CREATE, 0.05);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.RENAME_FILE, 0.05);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.MOVE_FILE, 0.05);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.DELETE_FILE, 0.05);
	}
	
	private long numberOfDirs;
	private long numberOfFiles;
	private IOperationDispatcher dispatcher;
	private OperationTypeSelector operationTypeSelector;
	private DirectoryAndFileSelector randomSelector;
	
	private boolean filesReadOnlyWorkload;
	private boolean dirsReadOnlyWorkload;

	private Set<Long> deletedFileIds;
	private IdCache accessedElementIdCache;
	
	public WorkloadGenerator(IOperationDispatcher dispatcher, int numberOfOperations, long numberOfDirs, long numberOfFiles){
		this.numberOfOperations = numberOfOperations;
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dispatcher = dispatcher;
		this.operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		this.randomSelector = new DirectoryAndFileSelector(numberOfDirs, numberOfFiles);
		
		filesReadOnlyWorkload = isFilesReadOnlyWorkload();
		System.out.println("files read only: " + filesReadOnlyWorkload);
		dirsReadOnlyWorkload = isDirsReadOnlyWorkload();
		int deleteOps = getNumberOfOperations(FileSystemOperationType.DELETE_FILE);
		int createOps = getNumberOfOperations(FileSystemOperationType.CREATE);
		if(deleteOps > createOps){
			int initialCapacity = Math.max(deleteOps - createOps, DELETED_FILE_SET_MAX_SIZE);
			this.deletedFileIds = new LinkedHashSet<Long>((int)(initialCapacity / (5 * 0.75)));
		} else {
			this.deletedFileIds = new LinkedHashSet<Long>();
		}
		this.accessedElementIdCache = new IdCache(ACCESSED_ELEMENT_CACHE_MAX_SIZE, ACCESSED_ELEMENT_CACHE_TTL);
	}
	
	private int getNumberOfOperations(FileSystemOperationType type){
		if(OPERATION_PROBABILITIES.containsKey(type)){
			double percentage = OPERATION_PROBABILITIES.get(type);
			return (int)(numberOfOperations * percentage);
		} else {
			return 0;
		}
	}
	
	private boolean isFilesReadOnlyWorkload(){
		if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.CREATE)){
			return false;
		} else if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.DELETE_FILE)) {
			return false;
		} else if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.RENAME_FILE)) {
			return false;
		} else if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.MOVE_FILE)) {
			return false;
		} else {
			return true;
		}
	}
	
	private boolean isDirsReadOnlyWorkload(){
		if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.MKDIRS)) {
			return false;
		} else {
			return true;
		}
	}
	
	public void generate(){
		int create = 0;
		int mkdir = 0;
		int delete = 0;
		int lsfile = 0;
		int lsdir = 0;
		int open = 0;
		int rename = 0;
		int move = 0;
		for(int i = 1; i <= numberOfOperations; i++){
			FileSystemOperationType operation = operationTypeSelector.getRandomOperationType();
			switch(operation){
				case CREATE:
					create++;
					create();
					break;
				case MKDIRS:
					mkdir++;
					mkdir();
					break;
				case DELETE_FILE:
					delete++;
					deleteFile();
					break;
				case LIST_STATUS_FILE:
					lsfile++;
					listStatusFile();
					break;
				case LIST_STATUS_DIR:
					lsdir++;
					listStatusDir();
					break;
				case OPEN_FILE:
					open++;
					openFile();
					break;
				case RENAME_FILE:
					rename++;
					renameFile();
					break;
				case MOVE_FILE:
					move++;
					moveFile();
					break;
				default:
					System.out.println("Invalid operation generated");
			}
			if((i % THROTTLE_AFTER_ITERATIONS) == 0){
				throttle();
			}
		}
		System.out.println("create: " + create);
		System.out.println("mkdir: " + mkdir);
		System.out.println("delete: " + delete);
		System.out.println("lsfile: " + lsfile);
		System.out.println("lsdir: " + lsdir);
		System.out.println("open: " + open);
		System.out.println("rename: " + rename);
		System.out.println("move: " + move);
	}
	
	private void throttle(){
		try {
			System.out.println("Going to sleep for " + THROTTLE_MILLIS);
			Thread.sleep(THROTTLE_MILLIS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private long getNewFileId(){
		if(!deletedFileIds.isEmpty()){
			Iterator<Long> iterator = deletedFileIds.iterator();
			Long id = iterator.next();
			while(iterator.hasNext() && accessedElementIdCache.containsFileId(id)){
				id = iterator.next();
			}
			return id;
		} else {
			return ++numberOfFiles;
		}
	}
	
	private long getRandomFileId(){
		long id = randomSelector.getRandomFile(numberOfFiles);
		while(deletedFileIds.contains(id) || accessedElementIdCache.containsFileId(id)){
			if(deletedFileIds.size() >= numberOfFiles){
				System.out.println("Fatal error: All files have been deleted.");
				System.exit(0);
			}
			id = randomSelector.getRandomFile(numberOfFiles);
		}
		return id;
	}
	
	private long getNewDirId(){
		return ++numberOfDirs;
	}
	
	private long getRandomDirId(){
		long id = randomSelector.getRandomDirectory(numberOfDirs);
		while(accessedElementIdCache.containsDirId(id)){
			id = randomSelector.getRandomDirectory(numberOfDirs);
		}
		return id;
	}
	
	private void create() {
		long parentId = randomSelector.getRandomDirectory(numberOfDirs);
		long id = getNewFileId();
		String name = FILE_NAME_PREFIX + id;
		SimpleOperation op = new CreateOperation(FileSystemOperationType.CREATE, parentId, id, name);
		dispatcher.dispatch(op);
		fileAccessed(id);
	}
	
	private void mkdir() {
		long parentId = randomSelector.getRandomDirectory(numberOfDirs);
		long id = getNewDirId();
		String name = DIR_NAME_PREFIX + id;
		SimpleOperation op = new CreateOperation(FileSystemOperationType.MKDIRS, parentId, id, name);
		dispatcher.dispatch(op);
		dirAccessed(id);
	}

	private void deleteFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.DELETE_FILE, id));
		deletedFileIds.add(id);
		fileAccessed(id);
	}

	private void listStatusFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.LIST_STATUS_FILE, id));
		fileAccessed(id);
	}
	
	private void listStatusDir() {
		long id = getRandomDirId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.LIST_STATUS_DIR, id));
		dirAccessed(id);
	}

	private void openFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.OPEN_FILE, id));
		fileAccessed(id);
	}

	private void renameFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.RENAME_FILE, id));
		fileAccessed(id);
	}
	
	private void moveFile() {
		long id = getRandomFileId();
		long parentIdNew = randomSelector.getRandomDirectory(numberOfDirs);
		dispatcher.dispatch(new MoveOperation(FileSystemOperationType.MOVE_FILE, id, parentIdNew));
		fileAccessed(id);
	}
	
	private void fileAccessed(long id){
		if(!filesReadOnlyWorkload){
			accessedElementIdCache.addFileId(id);
		}
	}
	
	private void dirAccessed(long id){
		if(!dirsReadOnlyWorkload){
			accessedElementIdCache.addDirId(id);
		}
	}
	
}
