package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class DirectoryAndFileSelector {
	
	private final ZipfianGenerator dirRNG;
	private final ZipfianGenerator fileRNG;
	
	public DirectoryAndFileSelector(long numberOfDirs, long numberOfFiles){
		this.dirRNG = new ZipfianGenerator(numberOfDirs);
		this.fileRNG = new ZipfianGenerator(numberOfFiles);
	}
	
	public long getRandomDirectory(long items){
		long id = dirRNG.nextLong() % items;
		return items - id;
	}
	
	public long getRandomFile(long items){
		long id = fileRNG.nextLong() % items;
		return items - id;
	}
	
}
