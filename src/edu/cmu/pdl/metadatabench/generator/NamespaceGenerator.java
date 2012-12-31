package edu.cmu.pdl.metadatabench.generator;

public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy dirCreator;
	private AbstractFileCreationStrategy fileCreator;
	
	public NamespaceGenerator(AbstractDirectoryCreationStrategy dirCreator, AbstractFileCreationStrategy fileCreator){
		this.dirCreator = dirCreator;
		this.fileCreator = fileCreator;
	}
	
	public void generateDirs(int numberOfDirs){
		dirCreator.createRoot();
		for(int i=2; i<numberOfDirs; i++){
			dirCreator.createNextDirectory();
		}
	}
	
	public void generateFiles(int numberOfFiles){
		for(int i=0; i<numberOfFiles; i++){
			fileCreator.createNextFile();
		}
	}

}
