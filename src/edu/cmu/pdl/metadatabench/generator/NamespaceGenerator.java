package edu.cmu.pdl.metadatabench.generator;

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
		for(int i=3+id; i < numberOfDirs; i+=masters){
			dirCreator.createNextDirectory(i);
		}
	}
	
	public void generateFiles(int numberOfFiles){
		for(int i=0; i<numberOfFiles; i++){
			fileCreator.createNextFile();
		}
	}

}
