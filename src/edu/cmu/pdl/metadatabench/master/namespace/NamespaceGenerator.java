package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.master.progress.Throttler;

/**
 * The namespace generator can generate a given amount of directories or files by calling the corresponding 
 * creation strategy.
 * 
 * @author emil.rakadjiev
 *
 */
public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy dirCreator;
	private AbstractFileCreationStrategy fileCreator;
	private int id;
	
	/**
	 * @param dirCreator The directory creation strategy
	 * @param fileCreator The file creation strategy
	 * @param id The id of this master node
	 */
	public NamespaceGenerator(AbstractDirectoryCreationStrategy dirCreator, AbstractFileCreationStrategy fileCreator, int id){
		this.dirCreator = dirCreator;
		this.fileCreator = fileCreator;
		this.id = id;
	}
	
	/**
	 * Generates a given number of directories by calling the directory creation strategy.
	 * 
	 * @param numberOfDirs The number of directories to create
	 */
	public void generateDirs(int numberOfDirs){
		// if this is the master node with id 0, create the root
		if(id == 0){
			dirCreator.createRoot();
		}
		for(int i=2; i <= numberOfDirs; i++){
			dirCreator.createNextDirectory(i);
			
			// If needed, throttle the generation, that is, wait for the generated operations to be executed by the slaves
			Throttler.throttle(i);
		}
	}
	
	/**
	 * Generates a given number of files by calling the file creation strategy.
	 * 
	 * @param numberOfFiles The number of files to create
	 */
	public void generateFiles(int numberOfFiles){
		for(int i=1; i <= numberOfFiles; i++){
			fileCreator.createNextFile(i);
			
			// If needed, throttle the generation, that is, wait for the generated operations to be executed by the slaves
			Throttler.throttle(i);
		}
	}
	
}
