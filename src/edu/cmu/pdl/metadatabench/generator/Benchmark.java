package edu.cmu.pdl.metadatabench.generator;

public class Benchmark {

	public static void main(String[] args) {
		int numberOfDirs = 0;
		int numberOfFiles = 0;
		int numberOfOperations = 0;
		
		if(args.length > 0){
			numberOfDirs = Integer.parseInt(args[0]);
			if(args.length > 1){
				numberOfFiles = Integer.parseInt(args[1]);
				if(args.length > 2){
					numberOfOperations = Integer.parseInt(args[2]);
				}
			}
		} else {
			System.out.println("Please enter parameters: benchmark numberOfDirs (numberOfFiles) (numberOfOperations)");
			System.exit(0);
		}
		
		INamespaceMapDAO dao = new HazelcastMapDAO();
		AbstractDirectoryCreationStrategy dirCreator = new BarabasiAlbertCreationStrategy(dao, "/workDir");
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator);
		long start = System.currentTimeMillis();
		nsGen.generateDirs(numberOfDirs);
		long end = System.currentTimeMillis();
		System.out.println("Dir time:" + (end-start)/1000.0);
		if(numberOfFiles > 0){
			start = System.currentTimeMillis();
			nsGen.generateFiles(numberOfFiles);
			end = System.currentTimeMillis();
			System.out.println("File time:" + (end-start)/1000.0);
		}
		dirCreator.testPrint();
		fileCreator.testPrint();
		System.out.println("===========================================");
		System.out.println("===========================================");
		System.out.println("===========================================");
		WorkloadGenerator wlGen = new WorkloadGenerator(dao, numberOfOperations, numberOfDirs, numberOfFiles);
		wlGen.generate();
		System.out.println(dao.getNumberOfDirs());
		System.out.println(dao.getNumberOfFiles());
	}

}
