package edu.cmu.pdl.metadatabench.master.namespace;

import com.yahoo.ycsb.generator.ZipfianGenerator;

import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;

public class ZipfianFileCreationStrategy extends AbstractFileCreationStrategy {

	private ZipfianGenerator randomGenerator;
	
	public ZipfianFileCreationStrategy(IOperationDispatcher dispatcher, long numberOfDirs){
		super(dispatcher, numberOfDirs);
		randomGenerator = new ZipfianGenerator(numberOfDirs);
	}
	
	@Override
	public long selectParentDirectory() {
		return numberOfDirs - randomGenerator.nextLong();
	}

}
