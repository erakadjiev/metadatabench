package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class ZipfianFileCreationStrategy extends AbstractFileCreationStrategy {

	private ZipfianGenerator randomGenerator;
	
	public ZipfianFileCreationStrategy(INamespaceMapDAO dao, long numberOfDirs){
		super(dao, numberOfDirs);
		randomGenerator = new ZipfianGenerator(numberOfDirs);
	}
	
	@Override
	public String selectDirectory() {
		long id = randomGenerator.nextLong();
		String dirPath = dao.getDir(numberOfDirs - id);
		return dirPath;
	}

}