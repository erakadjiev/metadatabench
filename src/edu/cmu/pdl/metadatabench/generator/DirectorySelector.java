package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class DirectorySelector {
	
	private final ZipfianGenerator randomNumberGenerator;
	
	public DirectorySelector(){
		this.randomNumberGenerator = new ZipfianGenerator(0);
	}
	
	public long getRandomDirectory(long items){
		long id = randomNumberGenerator.nextLong(items);
		return items - id;
	}
	
}
