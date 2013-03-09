package edu.cmu.pdl.metadatabench.master.workload;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Cache for accessed directory and file ids. The cache has a maximum size and the elements have an expiry time. 
 * Elements are evicted if one of the conditions is met.
 * 
 * @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize()
 * @author emil.rakadjiev
 *
 */
public class IdCache {

	private Map<Long, Object> dirCache;
	private Map<Long, Object> fileCache;
	
	/** Since the cache provided by the library is a map and we only need a set, set the value to a dummy object */
	private static final Object DUMMY = new Object();
	
	/**
	 * @param maxSize Maximum size of the cache
	 * @param expireAfterMillis Expiry time of the elements in the cache, after which they get evicted
	 */
	public IdCache(int maxSize, long expireAfterMillis) {
		this.dirCache = buildCache(maxSize, expireAfterMillis).asMap();
		this.fileCache = buildCache(maxSize, expireAfterMillis).asMap();
	}
	
	/**
	 * Builds a cache with the supplied parameters
	 * 
	 * @param maxSize Maximum size of the cache
	 * @param expireAfterMillis Expiry time of the elements in the cache, after which they get evicted
	 * @return The cache
	 */
	private Cache<Long, Object> buildCache(int maxSize, long expireAfterMillis){
		return CacheBuilder.newBuilder()
				.maximumSize(maxSize)
				.expireAfterWrite(expireAfterMillis, TimeUnit.MILLISECONDS)
				.concurrencyLevel(2)
				.build();
	}
	
	/**
	 * Adds a directory id to the cache
	 * @param id The directory id to add to the cache
	 */
	public void addDirId(long id){
		dirCache.put(id, DUMMY);
	}
	
	/**
	 * Adds a file id to the cache
	 * @param id The file id to add to the cache
	 */
	public void addFileId(long id){
		fileCache.put(id, DUMMY);
	}
	
	/**
	 * Checks whether a directory id is in the cache
	 * @param id The directory id to check
	 * @return True if the directory id is in the cache
	 */
	public boolean containsDirId(long id){
		return dirCache.containsKey(id);
	}
	
	/**
	 * Checks whether a file id is in the cache
	 * @param id The file id to check
	 * @return True if the file id is in the cache
	 */
	public boolean containsFileId(long id){
		return fileCache.containsKey(id);
	}

}
