package edu.cmu.pdl.metadatabench.master.namespace;

import com.yahoo.ycsb.generator.ZipfianGenerator;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;

/**
 * A file creation strategy that selects a parent directory from the existing set of directories 
 * using a Zipfian distribution.
 * 
 * @author emil.rakadjiev
 *
 */
public class ZipfianFileCreationStrategy extends AbstractFileCreationStrategy {

	private ZipfianGenerator randomGenerator;
	
	/**
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 * @param numberOfDirs The number of existing directories
	 */
	public ZipfianFileCreationStrategy(IDispatcher dispatcher, long numberOfDirs){
		super(dispatcher, numberOfDirs);
		randomGenerator = new ZipfianGenerator(numberOfDirs);
	}
	
	/**
	 * Selects an existing directory using Zipfian distribution as the parent for a new file.
	 * The Zipfian distribution chooses a small fraction of the elements with a large probability. 
	 * By default, the used random generator favors the lower end of the interval (around 0), but 
	 * the order is reversed and this method chooses the larger sequence numbers with large probability.
	 */
	@Override
	public long selectParentDirectory() {
		return numberOfDirs - randomGenerator.nextLong();
	}

}
