package edu.cmu.pdl.metadatabench.generator;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class ZipfianFileCreationStrategy extends AbstractFileCreationStrategy {

	private INamespaceMapDAO dao;
	private ZipfianGenerator randomGenerator;
	
	public ZipfianFileCreationStrategy(INamespaceMapDAO dao){
		super(dao);
		randomGenerator = new ZipfianGenerator(0);
	}
	
	@Override
	public String selectDirectory(long numberOfFiles) {
		long id = randomGenerator.nextLong(numberOfFiles);
		String dirPath = dao.getDir(id);
		return dirPath;
	}

}
