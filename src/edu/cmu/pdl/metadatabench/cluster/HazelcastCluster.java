package edu.cmu.pdl.metadatabench.cluster;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * Provides methods for administration (e.g. start, stop, join, etc) of a Hazelcast cluster.
 * 
 * 
 * @author emil.rakadjiev
 *
 */
public class HazelcastCluster implements ICluster {

	/** Singleton */
	private static final HazelcastCluster instance = new HazelcastCluster();
	/**
	 * Reference to the Hazelcast cluster instance.
	 * Theoretically there could be multiple Hazelcast instances in a JVM, but we ignore this use-case here.
	 */
	private static HazelcastInstance hazelcast;
	/**
	 * Reference to the master.
	 * If there are multiple masters, this is the leader, for example the one that collects and aggregates 
	 * measurements from all slaves.
	 */
	private static Member master;
	/** Set of all slaves */
	private static Set<Member> slaves;
	
	private HazelcastCluster() {}
	
	/**
	 * Gets the singleton cluster object
	 * 
	 * @return The cluster object
	 */
	public static HazelcastCluster getInstance() {
		return instance;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start() {
		// No need to explicitly start a Hazelcast cluser
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The master is a lite member (it does not store data).
	 */
	@Override
	public void joinAsMaster() {
		System.setProperty("hazelcast.lite.member", "true");
		Config config = new ClasspathXmlConfig("hazelcast-master.xml");
		hazelcast = Hazelcast.newHazelcastInstance(config);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void joinAsSlave() {
		System.clearProperty("hazelcast.lite.member");
		Config config = new ClasspathXmlConfig("hazelcast-slave.xml");
		hazelcast = Hazelcast.newHazelcastInstance(config);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * This does not guarantee that the cluster will be stopped. Sometimes only manually killing 
	 * the respective Java process helps.
	 */
	@Override
	public void stop() {
		if(hazelcast != null){
			hazelcast.getLifecycleService().shutdown();
			hazelcast.getLifecycleService().kill();
		} else {
			Hazelcast.shutdownAll();
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int generateMasterId() {
		AtomicNumber id = hazelcast.getAtomicNumber("id");
		return getNextId(id);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int generateSlaveId() {
		AtomicNumber id = hazelcast.getAtomicNumber("slaveid");
		return getNextId(id);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void masterJoined() {
		AtomicNumber masters = hazelcast.getAtomicNumber("masters");
		masters.incrementAndGet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void slaveJoined() {
		AtomicNumber slaves = hazelcast.getAtomicNumber("slaves");
		slaves.incrementAndGet();
	}
	
	/**
	 * Gets the next available id using a distributed atomic number.
	 * 
	 * @param id The atomic number to use for id generation
	 * @return The id
	 */
	private int getNextId(AtomicNumber id){
		int i = 0;
		int j = 1;
		while(!id.compareAndSet(i, j)){
			i++; j++;
		}
		return i;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Returns true if at least the given number of master and slaves have already joined the cluster.
	 */
	@Override
	public boolean allMembersJoined(int masters, int slaves) {
		Set<Member> members = hazelcast.getCluster().getMembers();
		boolean noSlavesJoined = true;
		for(Member member : members){
			if(!member.isLiteMember()){
				noSlavesJoined = false;
			}
		}
		
		if(!noSlavesJoined){
			int masterCount = (int)hazelcast.getAtomicNumber("masters").get();
			int slaveCount = (int)hazelcast.getAtomicNumber("slaves").get();
			
			return (masterCount >= masters) && (slaveCount >= slaves);
		} else {
			return false;
		}
	}
	
	/**
	 * Gets the reference to the Hazelcast instance
	 * 
	 * @return The reference to the Hazelcast instance
	 */
	public HazelcastInstance getHazelcast(){
		return hazelcast;
	}
	
	/**
	 * Gets the master member.
	 * If there are multiple masters, this is the leader, e.g. the one that collects and aggregates 
	 * measurements from all slaves.
	 * 
	 * @return The master member
	 */
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
	
	/**
	 * Gets the set of slave members.
	 * 
	 * @return The set of slave members.
	 */
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
