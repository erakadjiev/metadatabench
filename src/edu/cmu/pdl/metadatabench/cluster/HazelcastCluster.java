package edu.cmu.pdl.metadatabench.cluster;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

public class HazelcastCluster implements ICluster {

	private static final HazelcastCluster instance = new HazelcastCluster();
	private static HazelcastInstance hazelcast;
	private static Member master;
	private static Set<Member> slaves;
	
	private HazelcastCluster() {}

	public static HazelcastCluster getInstance() {
		return instance;
	}

	@Override
	public void start() {
		// No need to explicitly start a Hazelcast cluser
	}
	
	@Override
	public void joinAsMaster() {
		System.setProperty("hazelcast.lite.member", "true");
		Config config = new ClasspathXmlConfig("hazelcast-master.xml");
		hazelcast = Hazelcast.newHazelcastInstance(config);
	}
	
	@Override
	public void joinAsSlave() {
		System.clearProperty("hazelcast.lite.member");
		Config config = new ClasspathXmlConfig("hazelcast-node.xml");
		hazelcast = Hazelcast.newHazelcastInstance(config);
	}
	
	@Override
	public void stop() {
		hazelcast.getLifecycleService().kill();
	}
	
	@Override
	public int generateMasterId() {
		AtomicNumber id = hazelcast.getAtomicNumber("id");
		return getNextId(id);
	}
	
	@Override
	public int generateSlaveId() {
		AtomicNumber id = hazelcast.getAtomicNumber("slaveid");
		return getNextId(id);
	}
	
	private int getNextId(AtomicNumber id){
		int i = 0;
		int j = 1;
		while(!id.compareAndSet(i, j)){
			i++; j++;
		}
		return i;
	}
	
	@Override
	public boolean allMembersJoined(int masters, int slaves) {
		Set<Member> members = hazelcast.getCluster().getMembers();
		int masterCount = 0;
		int slaveCount = 0;
		for(Member member : members){
			if(member.isLiteMember()){
				masterCount++;
			} else {
				slaveCount++;
			}
		}
		return (masterCount == masters) && (slaveCount == slaves);
	}
	
	public HazelcastInstance getHazelcast(){
		return hazelcast;
	}
	
	public Member getMaster(){
		if(master == null){
			Set<Member> members = hazelcast.getCluster().getMembers();
			for(Member member : members){
				if(member.isLiteMember()){
					master = member;
				}
			}
		}
		return master;
	}
	
	public Set<Member> getSlaves(){
		if(slaves == null){
			slaves = new HashSet<Member>();
			Set<Member> members = hazelcast.getCluster().getMembers();
			for(Member member : members){
				if(!member.isLiteMember()){
					slaves.add(member);
				}
			}
		}
		return slaves;
	}

}
