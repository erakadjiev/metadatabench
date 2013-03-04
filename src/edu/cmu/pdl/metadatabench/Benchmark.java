package edu.cmu.pdl.metadatabench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.HazelcastCluster;
import edu.cmu.pdl.metadatabench.cluster.ICluster;
import edu.cmu.pdl.metadatabench.master.Master;
import edu.cmu.pdl.metadatabench.slave.Slave;

public class Benchmark {

	private static final int MASTERS = 1;
	private static final int NODES = 2;
	
	private static final int SLEEP_TIME = 2000;
	
	public static void main(String[] args) {
		
		ICluster cluster = HazelcastCluster.getInstance();
		
		if(args.length > 0){
			String mode = args[0];
			Logger log = LoggerFactory.getLogger(Benchmark.class);
			if(mode.equals("slave")){
				cluster.joinAsSlave();
				int id = cluster.generateSlaveId();
				Slave.start(((HazelcastCluster)cluster).getHazelcast(), id);
			} else if(mode.equals("master")){
				int numberOfDirs = 0;
				int numberOfFiles = 0;
				int numberOfOperations = 0;
				
				numberOfDirs = Integer.parseInt(args[1]);
				if(args.length > 2){
					numberOfFiles = Integer.parseInt(args[2]);
					if(args.length > 3){
						numberOfOperations = Integer.parseInt(args[3]);
					}
				}
				
				cluster.joinAsMaster();
				
				try {
					log.debug("Going to sleep for {} ms after joining cluster.", SLEEP_TIME);
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					log.warn("Thread interrupted while sleeping", e);
				}
				
				log.info("Waiting for all members to join the cluster.");

				while(!cluster.allMembersJoined(MASTERS, NODES)){
					try {
						log.debug("Going to sleep for {} ms after all members have cluster.", SLEEP_TIME);
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						log.warn("Thread interrupted while sleeping", e);
					}
				}
				
				log.info("All members joined the cluster. Starting the generation.");
				
				int id = cluster.generateMasterId();
				Master.start(((HazelcastCluster)cluster).getHazelcast(), id, MASTERS, numberOfDirs, numberOfFiles, numberOfOperations);
			} else if(mode.equalsIgnoreCase("stop")){
				cluster.stop();
				System.exit(0);
			} else {
				printUsage();
				System.exit(0);
			}
		} else {
			printUsage();
			System.exit(0);
		}
	}

	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println("metadatabench master numberOfDirs (numberOfFiles) (numberOfOperations)");
		System.out.println("Starts a master (generator) node");
		System.out.println("The last 3 parameters specify how many dirs/files/operations to generate");
		System.out.println("numberOfFiles and numberOfOperations are optional");
		System.out.println();
		System.out.println("metadatabench slave");
		System.out.println("Starts a storage and worker node");
		System.out.println();
		System.out.println("metadatabench stop");
		System.out.println("Tries to stop all the nodes on the current machine.");
		System.out.println("Sometimes only manually killing respective Java process helps.");
		System.out.println();
	}
	
}
