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
import edu.cmu.pdl.metadatabench.master.progress.Throttler;

/**
 * The workload generator generates and dispatches a given amount of operations.
 * 
 * In each step, an operation type is randomly selected according to a user-specified probability distribution. 
 * It chooses the needed parameters for the respective operation, for example the parent directory id, 
 * the new id, the name, etc, and dispatches the operation to a slave.
 * 
 * The accessed elements' ids are cached for a given time, in order to prevent further access to them that 
 * could cause conflicts. The reason is that the benchmark is distributed and operates asynchronously and 
 * thus the workload operation generation is not the same as the execution order. For example an open file20 
 * operation can be generated, followed by a delete file20. Both are dispatched and the delete operation 
 * could get executed before the open, leading to an error.
 * The deleted elements are tracked as well. Their ids cannot be be used, except when reusing them for newly created 
 * elements.
 * 
 * @author emil.rakadjiev
 *
 */
public class WorkloadGenerator {

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize() */
	private static final int ACCESSED_ELEMENT_CACHE_MAX_SIZE = Config.getWorkloadAccessedElementCacheMaxSize();
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheTTL() */
	private static final long ACCESSED_ELEMENT_CACHE_TTL = Config.getWorkloadAccessedElementCacheTTL();
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getPathSeparator() */
	protected static char PATH_SEPARATOR = Config.getPathSeparator();
	/** @see edu.cmu.pdl.metadatabench.common.Config#getDirNamePrefix() */
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + Config.getDirNamePrefix();
	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileNamePrefix() */
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + Config.getFileNamePrefix();
	
	/** The number of operations to generate */
	private int numberOfOperations;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadOperationProbabilities() */
	private static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = Config.getWorkloadOperationProbabilities();
	
	/** The number of existing directories in the namespace */
	private long numberOfDirs;
	/** The number of existing files in the namespace */
	private long numberOfFiles;
	private IDispatcher dispatcher;
	private OperationTypeSelector operationTypeSelector;
	private ZipfianDirectoryAndFileIdSelector randomSelector;
	
	/** 
	 * True if no operations are present in the workload that modify files. In that case, 
	 * no caching of accessed file ids is needed, because no conflicts can arise.
	 */
	private boolean filesReadOnlyWorkload;
	/** 
	 * True if no operations are present in the workload that modify directories. In that case, 
	 * no caching of accessed directory ids is needed, because no conflicts can arise.
	 */
	private boolean dirsReadOnlyWorkload;

	/** Set of ids of deleted files. These ids can be reused for newly created files. */
	private Set<Long> deletedFileIds;
	/** 
	 * Caches the accessed elements' ids for a given time, in order to prevent further access to them that 
	 * could cause conflicts.
	 */
	private IdCache accessedElementIdCache;
	
	private Logger log;
	
	/**
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 * @param numberOfOperations The number of workload operations to be generated
	 * @param numberOfDirs The number of existing directories in the namespace
	 * @param numberOfFiles The number of existing files in the namespace
	 */
	public WorkloadGenerator(IDispatcher dispatcher, int numberOfOperations, long numberOfDirs, long numberOfFiles){
		this.numberOfOperations = numberOfOperations;
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dispatcher = dispatcher;
		this.operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		this.randomSelector = new ZipfianDirectoryAndFileIdSelector(numberOfDirs, numberOfFiles);
		
		filesReadOnlyWorkload = isFilesReadOnlyWorkload();
		dirsReadOnlyWorkload = isDirsReadOnlyWorkload();
		int deleteOps = getNumberOfOperations(FileSystemOperationType.DELETE_FILE);
		int createOps = getNumberOfOperations(FileSystemOperationType.CREATE);
		/*
		 * The set of deleted file ids is a linked hash set. This data structure offers fast, constant-time 
		 * performance for add and contains operations (like a hash set), which is important during operation 
		 * generation, because deleted ids have to be saved and selected ids have to be checked, whether they 
		 * have been deleted previously. Furthermore, the linked hash set provides a generally faster iteration 
		 * speed than a hash set and FIFO iteration order, which is useful when selecting ids to reuse. 
		 * The older ids are selected first and they are less likely to be still in the accessed element cache. 
		 *
		 * If there are more delete operations than create operations in the workload, initialize the hash set 
		 * with larger capacity to avoid too many rehashings when the capacity is automatically increased.
		 */
		if(deleteOps > createOps){
			/*
			 * The initial capacity is chosen to be 1/5th (random choice) of the delete operations, because 
			 * starting off with a too large capacity may negatively affect performance. Furthermore, create 
			 * operations remove elements from the set, so that capacity may never be reached.
			 * 0.75 is the optimal load factor.
			 *   
			 */
			this.deletedFileIds = new LinkedHashSet<Long>((int)(deleteOps / (5 * 0.75)));
		} else {
			this.deletedFileIds = new LinkedHashSet<Long>();
		}
		this.accessedElementIdCache = new IdCache(ACCESSED_ELEMENT_CACHE_MAX_SIZE, ACCESSED_ELEMENT_CACHE_TTL);
		
		log = LoggerFactory.getLogger(WorkloadGenerator.class);
	}
	
	/**
	 * Gets the approximate number of operations of a given type that will be generated in the workload
	 * 
	 * @param type The operation type
	 * @return The approximate number of operations of the given type that will be generated in the workload
	 */
	private int getNumberOfOperations(FileSystemOperationType type){
		if(OPERATION_PROBABILITIES.containsKey(type)){
			double percentage = OPERATION_PROBABILITIES.get(type);
			return (int)(numberOfOperations * percentage);
		} else {
			return 0;
		}
	}
	
	/**
	 * Returns true if no operations are present in the workload that modify files. In that case, 
	 * no caching of accessed file ids is needed, because no conflicts can arise.
	 * 
	 * @return True if no operations are present in the workload that modify files
	 */
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
	
	/**
	 * Returns true if no operations are present in the workload that modify directories. In that case, 
	 * no caching of accessed directory ids is needed, because no conflicts can arise.
	 * 
	 * @return True if no operations are present in the workload that modify directories
	 */
	private boolean isDirsReadOnlyWorkload(){
		if(OPERATION_PROBABILITIES.containsKey(FileSystemOperationType.MKDIRS)) {
			return false;
		} else {
			return true;
		}
	}
	
	/**
	 * Generates and dispatches a pre-defined amount of operations.
	 * In each step, an operation type is randomly selected according to a user-specified probability distribution. 
	 * It chooses the needed parameters for the respective operation, for example the parent directory id, 
	 * the new id, the name, etc, and dispatches the operation to a slave.
	 */
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
			Throttler.throttle(i);
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
	
	/**
	 * Gets an id for a new file. First it tries to reuse an id of a deleted file (that is not in the 
	 * accessed element cache anymore). If that fails, it simply takes the next highest sequence number 
	 * that has not been assigned yet.
	 *  
	 * @return The id for the new file
	 */
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
	
	/**
	 * Selects the id of a random file that has not been deleted and has not been accessed recently
	 * 
	 * @return The id of a random, existing file
	 */
	private long getRandomFileId(){
		long id = randomSelector.getRandomFileId(numberOfFiles);
		while(deletedFileIds.contains(id) || accessedElementIdCache.containsFileId(id)){
			if(deletedFileIds.size() >= numberOfFiles){
				log.error("Error: All files have been deleted while executing the workload.");
				System.exit(0);
			}
			id = randomSelector.getRandomFileId(numberOfFiles);
		}
		return id;
	}
	
	/**
	 * Gets an id for a new directory. Because currently directories are not deleted, simply the next highest 
	 * sequence number is returned.
	 * 
	 * @return The id for the new directory
	 */
	private long getNewDirId(){
		return ++numberOfDirs;
	}
	
	/**
	 * Selects the id of a random directory that has not been accessed recently
	 * 
	 * @return The id of a random directory
	 */
	private long getRandomDirId(){
		long id = randomSelector.getRandomDirectoryId(numberOfDirs);
		while(accessedElementIdCache.containsDirId(id)){
			id = randomSelector.getRandomDirectoryId(numberOfDirs);
		}
		return id;
	}
	
	/**
	 * Constructs and dispatches a create operation. Selects a parent directory id, an id for the new file and 
	 * creates its name.
	 */
	private void create() {
		long parentId = randomSelector.getRandomDirectoryId(numberOfDirs);
		long id = getNewFileId();
		String name = FILE_NAME_PREFIX + id;
		SimpleOperation op = new CreateOperation(FileSystemOperationType.CREATE, parentId, id, name);
		dispatcher.dispatch(op);
		fileAccessed(id);
	}
	
	/**
	 * Constructs and dispatches a mkdir operation. Selects a parent directory id, an id for the new directory and 
	 * creates its name.
	 */
	private void mkdir() {
		long parentId = randomSelector.getRandomDirectoryId(numberOfDirs);
		long id = getNewDirId();
		String name = DIR_NAME_PREFIX + id;
		SimpleOperation op = new CreateOperation(FileSystemOperationType.MKDIRS, parentId, id, name);
		dispatcher.dispatch(op);
		dirAccessed(id);
	}

	/**
	 * Constructs and dispatches a delete operation. Selects a random file id.
	 */
	private void deleteFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.DELETE_FILE, id));
		deletedFileIds.add(id);
		fileAccessed(id);
	}

	/**
	 * Constructs and dispatches an ls file operation. Selects a random file id. 
	 */
	private void listStatusFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.LIST_STATUS_FILE, id));
		fileAccessed(id);
	}
	
	/**
	 * Constructs and dispatches an ls dir operation. Selects a random directory.
	 */
	private void listStatusDir() {
		long id = getRandomDirId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.LIST_STATUS_DIR, id));
		dirAccessed(id);
	}

	/**
	 * Constructs and dispatches an open file operation. Selects a random file.
	 */
	private void openFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.OPEN_FILE, id));
		fileAccessed(id);
	}

	/**
	 * Constructs and dispatches a rename operation. Selects a random file.
	 */
	private void renameFile() {
		long id = getRandomFileId();
		dispatcher.dispatch(new SimpleOperation(FileSystemOperationType.RENAME_FILE, id));
		fileAccessed(id);
	}
	
	/**
	 * Constructs and dispatches a move operation. Selects a random file and a random new parent directory.
	 */
	private void moveFile() {
		long id = getRandomFileId();
		long parentIdNew = randomSelector.getRandomDirectoryId(numberOfDirs);
		dispatcher.dispatch(new MoveOperation(FileSystemOperationType.MOVE_FILE, id, parentIdNew));
		fileAccessed(id);
	}
	
	/**
	 * Add the file id to the accessed file ids' cache, except if the workload is read-only for files.
	 * @param id The id of the file to add to the accessed file ids' cache
	 */
	private void fileAccessed(long id){
		if(!filesReadOnlyWorkload){
			accessedElementIdCache.addFileId(id);
		}
	}
	
	/**
	 * Add the directory id to the accessed directory ids' cache, except if the workload is read-only for directories.
	 * @param id The id of the directory to add to the accessed directory ids' cache
	 */
	private void dirAccessed(long id){
		if(!dirsReadOnlyWorkload){
			accessedElementIdCache.addDirId(id);
		}
	}
	
}
