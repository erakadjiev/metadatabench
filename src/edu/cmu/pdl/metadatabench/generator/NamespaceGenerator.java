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
	
	public static void main(String[] args){
		long start = System.currentTimeMillis();
		INamespaceMapEntryDAO dao = new HazelcastMapEntryDAO();
		AbstractDirectoryCreationStrategy dirCreator = new BarabasiAlbertCreationStrategy(dao, "/workDir");
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao);
		NamespaceGenerator gen = new NamespaceGenerator(dirCreator, fileCreator);
		gen.generateDirs(1000000);
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);
		dirCreator.testPrint();
		fileCreator.testPrint();
	}

}
