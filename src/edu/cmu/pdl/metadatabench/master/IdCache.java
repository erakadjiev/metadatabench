package edu.cmu.pdl.metadatabench.master;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class IdCache {

	private Map<Long, Object> dirCache;
	private Map<Long, Object> fileCache;
	
	private static final Object DUMMY = new Object();
	
	public IdCache(int maxSize, long expireAfterMillis) {
		this.dirCache = buildCache(maxSize, expireAfterMillis).asMap();
		this.fileCache = buildCache(maxSize, expireAfterMillis).asMap();
	}
	
	private Cache<Long, Object> buildCache(int maxSize, long expireAfterMillis){
		return CacheBuilder.newBuilder()
				.maximumSize(maxSize)
				.expireAfterWrite(expireAfterMillis, TimeUnit.MILLISECONDS)
				.concurrencyLevel(2)
				.build();
	}
	
	public void addDirId(long id){
		dirCache.put(id, DUMMY);
	}
	
	public void addFileId(long id){
		fileCache.put(id, DUMMY);
	}
	
	public boolean containsDirId(long id){
		return dirCache.containsKey(id);
	}
	
	public boolean containsFileId(long id){
		return fileCache.containsKey(id);
	}

}
