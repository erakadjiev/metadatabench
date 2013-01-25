package edu.cmu.pdl.metadatabench.master;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractDirectoryCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.NamespaceGenerator;
import edu.cmu.pdl.metadatabench.master.namespace.UniformCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.ZipfianFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.workload.WorkloadGenerator;

public class Master {

	public static void start(HazelcastInstance hazelcast, int masters, int numberOfDirs, int numberOfFiles, int numberOfOperations){
		INamespaceMapDAO dao = new HazelcastMapDAO(hazelcast);
		IOperationDispatcher dispatcher = new HazelcastDispatcher(hazelcast);
//		ICountDownLatch latch = hazelcast.getCountDownLatch("latch"); 
		
		AtomicNumber id = hazelcast.getAtomicNumber("id");
		int i = 0;
		int j = 1;
		while(!id.compareAndSet(i, j)){
			i++; j++;
		}
		int ownId = i;
		System.out.println("own id: " + ownId);
		
		AbstractDirectoryCreationStrategy dirCreator = new UniformCreationStrategy(dao, "/workDir", masters);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, ownId, masters);
		
		System.out.println("Dir creation started");
//		latch.setCount(numberOfDirs);
		long start = System.currentTimeMillis();
		nsGen.generateDirs(numberOfDirs/masters);
//		awaitLatch(latch);
		long end = System.currentTimeMillis();
		System.out.println("Dir time:" + (end-start)/1000.0);
		
		if(numberOfFiles > 0){
			System.out.println("File creation started");
//			latch.setCount(numberOfFiles);
			start = System.currentTimeMillis();
			nsGen.generateFiles(numberOfFiles);
//			awaitLatch(latch);
			end = System.currentTimeMillis();
			System.out.println("File time:" + (end-start)/1000.0);
		}
		
		if(numberOfOperations > 0){
			System.out.println("Workload generation started");
			WorkloadGenerator wlGen = new WorkloadGenerator(dao, dispatcher, numberOfOperations, numberOfDirs, numberOfFiles);
			wlGen.generate();
			System.out.println(dao.getNumberOfDirs());
			System.out.println(dao.getNumberOfFiles());
		}
	}

//	private static void awaitLatch(ICountDownLatch latch){
//		try {
//			latch.await();
//		} catch (MemberLeftException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InstanceDestroyedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

}
