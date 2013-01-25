package edu.cmu.pdl.metadatabench.master.namespace;

import com.yahoo.ycsb.generator.ZipfianGenerator;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;

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
