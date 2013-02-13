package edu.cmu.pdl.metadatabench.master;

import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.ProgressReset;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractDirectoryCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.NamespaceGenerator;
import edu.cmu.pdl.metadatabench.master.namespace.UniformCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.ZipfianFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.workload.WorkloadGenerator;

public class Master {

	public static void start(HazelcastInstance hazelcast, int id, int masters, int numberOfDirs, int numberOfFiles, int numberOfOperations){
		INamespaceMapDAO dao = new HazelcastMapDAO(hazelcast);
		IOperationDispatcher dispatcher = new HazelcastDispatcher(hazelcast);
		
		AbstractDirectoryCreationStrategy dirCreator = new UniformCreationStrategy(dao, dispatcher, "/workDir", masters);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, id, masters);
		
		if(numberOfDirs > 0){
			System.out.println("Dir creation started");
			long start = System.currentTimeMillis();
			nsGen.generateDirs(numberOfDirs);
			long end = System.currentTimeMillis();
			System.out.println(numberOfDirs + " dirs generated in: " + (end-start)/1000.0);
			try {
				ProgressBarrier.awaitOperationCompletion(numberOfDirs);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			double creationTime = (System.currentTimeMillis()-start)/1000.0;
			System.out.println("Dir creation done in: " + creationTime + " s");
			System.out.println("Throughput: " + (numberOfDirs/creationTime) + " ops/s");
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
		}
		
		if(numberOfFiles > 0){
			System.out.println("File creation started");
			long start = System.currentTimeMillis();
			nsGen.generateFiles(numberOfFiles);
			long end = System.currentTimeMillis();
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

}
