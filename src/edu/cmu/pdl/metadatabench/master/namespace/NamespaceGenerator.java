package edu.cmu.pdl.metadatabench.master.namespace;

import edu.cmu.pdl.metadatabench.master.progress.Throttler;

public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy dirCreator;
	private AbstractFileCreationStrategy fileCreator;
	private int id;
	private int masters;
	
	public NamespaceGenerator(AbstractDirectoryCreationStrategy dirCreator, AbstractFileCreationStrategy fileCreator, int id, int masters){
		this.dirCreator = dirCreator;
		this.fileCreator = fileCreator;
		this.id = id;
		this.masters = masters;
	}
	
	public void generateDirs(int numberOfDirs){
		if(id == 0){
			dirCreator.createRoot();
		}
		for(int i=2+id; i <= numberOfDirs; i+=masters){
			dirCreator.createNextDirectory(i);
			
			Throttler.throttle(i);
		}
	}
	
	public void generateFiles(int numberOfFiles){
		for(int i=1+id; i <= numberOfFiles; i+=masters){
			fileCreator.createNextFile(i);
			
			Throttler.throttle(i);
		}
	}
	
}
