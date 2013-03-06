package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.master.progress.Throttler;

public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy dirCreator;
	private AbstractFileCreationStrategy fileCreator;
	private int id;
	
	public NamespaceGenerator(AbstractDirectoryCreationStrategy dirCreator, AbstractFileCreationStrategy fileCreator, int id){
		this.dirCreator = dirCreator;
		this.fileCreator = fileCreator;
		this.id = id;
	}
	
	public void generateDirs(int numberOfDirs){
		if(id == 0){
			dirCreator.createRoot();
		}
		for(int i=2; i <= numberOfDirs; i++){
			dirCreator.createNextDirectory(i);
			
			Throttler.throttle(i);
		}
	}
	
	public void generateFiles(int numberOfFiles){
		for(int i=1; i <= numberOfFiles; i++){
			fileCreator.createNextFile(i);
			
			Throttler.throttle(i);
		}
	}
	
}
