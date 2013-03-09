package edu.cmu.pdl.metadatabench.master.workload;

/**
 * Provides functionality to select a random id of an existing directory or file using a given probability distribution.
 * 
 * @author emil.rakadjiev
 *
 */
public interface IDirectoryAndFileIdSelector {

	/**
	 * Selects a random id of an existing directory using a given probability distribution
	 * 
	 * @param items The number of directories in the namespace
	 * @return The random directory id
	 */
	public long getRandomDirectoryId(long items);
	
	/**
	 * Selects a random id of an existing file using a given probability distribution
	 * 
	 * @param items The number of files in the namespace
	 * @return The random file id
	 */
	public long getRandomFileId(long items);
	
}
