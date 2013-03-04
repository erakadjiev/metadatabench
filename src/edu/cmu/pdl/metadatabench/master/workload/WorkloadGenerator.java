package edu.cmu.pdl.metadatabench.master.workload;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MoveOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

public class WorkloadGenerator {

	private static final int DELETED_FILE_SET_MAX_SIZE = Config.getWorkloadDeletedFileSetMaxSize();
	private static final int ACCESSED_ELEMENT_CACHE_MAX_SIZE = Config.getWorkloadAccessedElementCacheMaxSize();
	private static final long ACCESSED_ELEMENT_CACHE_TTL = Config.getWorkloadAccessedElementCacheTTL();
	private static final int THROTTLE_AFTER_ITERATIONS = Config.getWorkloadThrottleAfterIterations();
	private static final int THROTTLE_MILLIS = Config.getWorkloadThrottleMillis();
	
	protected static char PATH_SEPARATOR = Config.getPathSeparator();
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + Config.getDirNamePrefix();
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + Config.getFileNamePrefix();
	
	private int numberOfOperations;
	private static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = Config.getWorkloadOperationProbabilities();
	
	private long numberOfDirs;
	private long numberOfFiles;
	private IDispatcher dispatcher;
	private OperationTypeSelector operationTypeSelector;
	private DirectoryAndFileSelector randomSelector;
	
	private boolean filesReadOnlyWorkload;
	private boolean dirsReadOnlyWorkload;

	private Set<Long> deletedFileIds;
	private IdCache accessedElementIdCache;
	
	private Logger log;
	
	public WorkloadGenerator(IDispatcher dispatcher, int numberOfOperations, long numberOfDirs, long numberOfFiles){
		this.numberOfOperations = numberOfOperations;
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dispatcher = dispatcher;
		this.operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		this.randomSelector = new DirectoryAndFileSelector(numberOfDirs, numberOfFiles);
		
		filesReadOnlyWorkload = isFilesReadOnlyWorkload();
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
		
		log = LoggerFactory.getLogger(WorkloadGenerator.class);
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
					log.warn("Internal error: Invalid operation type generated");
			}
			if((i % THROTTLE_AFTER_ITERATIONS) == 0){
				throttle();
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append("Number of each operation type in generated workload:");
		sb.append("\n");
		sb.append("create: ");
		sb.append(create);
		sb.append("\n");
		sb.append("mkdir: ");
		sb.append(mkdir);
		sb.append("\n");
		sb.append("delete: ");
		sb.append(delete);
		sb.append("\n");
		sb.append("lsfile: ");
		sb.append(lsfile);
		sb.append("\n");
		sb.append("lsdir: ");
		sb.append(lsdir);
		sb.append("\n");
		sb.append("open: ");
		sb.append(open);
		sb.append("\n");
		sb.append("rename: ");
		sb.append(rename);
		sb.append("\n");
		sb.append("move: ");
		sb.append(move);
		log.debug(sb.toString());
	}
	
	private void throttle(){
		try {
			log.debug("Going to sleep for {} ms", THROTTLE_MILLIS);
			Thread.sleep(THROTTLE_MILLIS);
		} catch (InterruptedException e) {
			log.warn("Thread was interrupted while sleeping", e);
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
				log.error("Error: All files have been deleted while executing the workload.");
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
