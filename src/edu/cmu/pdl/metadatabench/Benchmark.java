package edu.cmu.pdl.metadatabench;

import edu.cmu.pdl.metadatabench.cluster.HazelcastCluster;
import edu.cmu.pdl.metadatabench.cluster.ICluster;
import edu.cmu.pdl.metadatabench.master.Master;
import edu.cmu.pdl.metadatabench.slave.Slave;

public class Benchmark {

	private static final int MASTERS = 1;
	private static final int NODES = 2;
	
	public static void main(String[] args) {
		
		ICluster cluster = HazelcastCluster.getInstance();
		
		if(args.length > 0){
			String mode = args[0];
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
				int id = cluster.generateMasterId();
				System.out.println("Waiting for all members to join the cluster.");

				while(!cluster.allMembersJoined(MASTERS, NODES)){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				System.out.println("All members joined the cluster. Starting the generation.");
				
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
		System.out.println("Usage: metadatabench master|slave|stop numberOfDirs (numberOfFiles) (numberOfOperations)");
		System.out.println();
		System.out.println("master starts a master (generator) node");
		System.out.println("slave starts a storage and worker node");
		System.out.println("stop stops all the nodes on the current machine");
		System.out.println();
		System.out.println("The last 3 parameters specify how many dirs/files/operations to generate");
		System.out.println("numberOfFiles and numberOfOperations are optional");
	}
	
}
