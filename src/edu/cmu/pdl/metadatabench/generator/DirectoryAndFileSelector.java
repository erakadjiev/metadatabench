package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class DirectoryAndFileSelector {
	
	private final ZipfianGenerator dirRNG;
	private final ZipfianGenerator fileRNG;
	
	public DirectoryAndFileSelector(){
		this.dirRNG = new ZipfianGenerator(0);
		this.fileRNG = new ZipfianGenerator(0);
	}
	
	public long getRandomDirectory(long items){
		long id = dirRNG.nextLong(items);
		return items - id;
	}
	
	public long getRandomFile(long items){
		long id = fileRNG.nextLong(items);
		return items - id;
	}
	
}
