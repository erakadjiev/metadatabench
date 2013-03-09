package edu.cmu.pdl.metadatabench.master.workload;

import com.yahoo.ycsb.generator.ZipfianGenerator;

/**
 * Provides functionality to select a random id of an existing directory or file using Zipfian distribution.
 * 
 * @author emil.rakadjiev
 *
 */
public class ZipfianDirectoryAndFileIdSelector implements IDirectoryAndFileIdSelector {
	
	private final ZipfianGenerator dirRNG;
	private final ZipfianGenerator fileRNG;
	private final long numberOfDirs;
	private final long numberOfFiles;
	
	/**
	 * @param numberOfDirs The number of directories in the namespace
	 * @param numberOfFiles The number of files in the namespace
	 */
	public ZipfianDirectoryAndFileIdSelector(long numberOfDirs, long numberOfFiles){
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dirRNG = new ZipfianGenerator(numberOfDirs);
		this.fileRNG = new ZipfianGenerator(numberOfFiles);
	}
	
	/**
	 * Selects a random id of an existing directory using Zipfian distribution.
	 * 
	 * @param items The number of directories in the namespace
	 * @return The random directory id
	 */
	@Override
	public long getRandomDirectoryId(long items){
		/*
		 * Re-initializing the Zipfian generator with a new interval is very expensive, so we just scale the 
		 * random number to the new interval.
		 */
		double scaling = items/numberOfDirs; 
		long id = (long)(dirRNG.nextLong() * scaling);
		return items - id;
	}
	
	/**
	 * Selects a random id of an existing file using Zipfian distribution.
	 * 
	 * @param items The number of files in the namespace
	 * @return The random files id
	 */
	@Override
	public long getRandomFileId(long items){
		/*
		 * Re-initializing the Zipfian generator with a new interval is very expensive, so we just scale the 
		 * random number to the new interval.
		 */
		double scaling = items/numberOfFiles; 
		long id = (long)(fileRNG.nextLong() * scaling);
		return items - id;
	}
	
}
