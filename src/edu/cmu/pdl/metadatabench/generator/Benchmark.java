package edu.cmu.pdl.metadatabench.generator;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.MemberLeftException;

public class Benchmark {

	public static void main(String[] args) {
		int numberOfDirs = 0;
		int numberOfFiles = 0;
		int numberOfOperations = 0;
		
		if(args.length > 0){
			if(args[0].equalsIgnoreCase("stop")){
				stopCluster();
				System.exit(0);
			}
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
		ICountDownLatch latch = ((HazelcastMapDAO)dao).getHazelcastInstance().getCountDownLatch("latch"); 
		AbstractDirectoryCreationStrategy dirCreator = new UniformCreationStrategy(dao, "/workDir");
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator);
		
		System.out.println("Dir creation started");
		latch.setCount(numberOfDirs);
		long start = System.currentTimeMillis();
		nsGen.generateDirs(numberOfDirs);
		awaitLatch(latch);
		long end = System.currentTimeMillis();
		System.out.println("Dir time:" + (end-start)/1000.0);
		
		if(numberOfFiles > 0){
			System.out.println("File creation started");
			latch.setCount(numberOfFiles);
			start = System.currentTimeMillis();
			nsGen.generateFiles(numberOfFiles);
			awaitLatch(latch);
			end = System.currentTimeMillis();
			System.out.println("File time:" + (end-start)/1000.0);
		}
		
		if(numberOfOperations > 0){
			System.out.println("Workload generation started");
			WorkloadGenerator wlGen = new WorkloadGenerator(dao, numberOfOperations, numberOfDirs, numberOfFiles);
			wlGen.generate();
			System.out.println(dao.getNumberOfDirs());
			System.out.println(dao.getNumberOfFiles());
		}
	}
	
	private static void stopCluster() {
		// TODO extract and make generic
		Hazelcast.shutdownAll();
		
	}

	private static void awaitLatch(ICountDownLatch latch){
		try {
			latch.await();
		} catch (MemberLeftException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstanceDestroyedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
