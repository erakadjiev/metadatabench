package edu.cmu.pdl.metadatabench.generator;

import java.util.Set;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.InstanceDestroyedException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

public class Benchmark {

	private static INamespaceMapDAO dao;
	private static int numberOfDirs = 0;
	private static int numberOfFiles = 0;
	private static int numberOfOperations = 0;
	
	private static final int MASTERS = 2;
	private static final int NODES = 2;
	
	public static void main(String[] args) {
		
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
		
		
		dao = new HazelcastMapDAO();
		final HazelcastInstance hazelcast = ((HazelcastMapDAO)dao).getHazelcastInstance();
		
		if(allMembersJoined(hazelcast, MASTERS, NODES)){
			System.out.println("All members joined the cluster. Starting the generation.");
			start();
		} else {
			MembershipListener memberListener = new MembershipListener() {
				@Override
				public void memberRemoved(MembershipEvent arg0) {
					// TODO Auto-generated method stub
				}
				
				@Override
				public void memberAdded(MembershipEvent arg0) {
					if(allMembersJoined(hazelcast, MASTERS, NODES)){
						hazelcast.getCluster().removeMembershipListener(this);
						System.out.println("All members joined the cluster. Starting the generation.");
						start();
					}
				}
			};
			
			hazelcast.getCluster().addMembershipListener(memberListener);
		}
		
		
	}
	
	private static void start(){
		HazelcastInstance hazelcast = ((HazelcastMapDAO)dao).getHazelcastInstance();
//		ICountDownLatch latch = hazelcast.getCountDownLatch("latch"); 
		
		AtomicNumber id = hazelcast.getAtomicNumber("id");
		int i = 0;
		int j = 1;
		while(!id.compareAndSet(i, j)){
			i++; j++;
		}
		int ownId = i;
		System.out.println("own id: " + ownId);
		
		AbstractDirectoryCreationStrategy dirCreator = new UniformCreationStrategy(dao, "/workDir", MASTERS);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dao, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, ownId, MASTERS);
		
		System.out.println("Dir creation started");
//		latch.setCount(numberOfDirs);
		long start = System.currentTimeMillis();
		nsGen.generateDirs(numberOfDirs/MASTERS);
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
	
	private static boolean allMembersJoined(HazelcastInstance hazelcast, int masters, int nodes){
		Set<Member> members = hazelcast.getCluster().getMembers();
		int masterCount = 0;
		int nodeCount = 0;
		for(Member member : members){
			if(member.isLiteMember()){
				masterCount++;
			} else {
				nodeCount++;
			}
		}
		return (masterCount == masters) && (nodeCount == nodes);
	}
}
