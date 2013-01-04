package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class DirectoryAndFileSelector {
	
	private final ZipfianGenerator dirRNG;
	private final ZipfianGenerator fileRNG;
	private final long numberOfDirs;
	private final long numberOfFiles;
	
	public DirectoryAndFileSelector(long numberOfDirs, long numberOfFiles){
		this.numberOfDirs = numberOfDirs;
		this.numberOfFiles = numberOfFiles;
		this.dirRNG = new ZipfianGenerator(numberOfDirs);
		this.fileRNG = new ZipfianGenerator(numberOfFiles);
	}
	
	public long getRandomDirectory(long items){
		double scaling = items/numberOfDirs; 
		long id = (long)(dirRNG.nextLong() * scaling);
		return items - id;
	}
	
	public long getRandomFile(long items){
		double scaling = items/numberOfFiles; 
		long id = (long)(fileRNG.nextLong() * scaling);
		return items - id;
	}
	
}
