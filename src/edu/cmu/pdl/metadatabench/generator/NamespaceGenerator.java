package edu.cmu.pdl.metadatabench.generator;

public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy creator;
	
	public NamespaceGenerator(AbstractDirectoryCreationStrategy creator){
		this.creator = creator;
	}
	
	public void generate(int numberOfDirs){
		creator.createRoot();
		for(int i=2; i<numberOfDirs; i++){
			creator.createNextDirectory();
		}
	}
	
	public static void main(String[] args){
		long start = System.currentTimeMillis();
		AbstractDirectoryCreationStrategy creator = new BarabasiAlbertCreationStrategy("/workDir");
		NamespaceGenerator gen = new NamespaceGenerator(creator);
		gen.generate(1000000);
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);
		creator.testPrint();
	}

}
