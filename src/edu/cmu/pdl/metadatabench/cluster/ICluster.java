package edu.cmu.pdl.metadatabench.cluster;

/**
 * Provides methods for cluster administration, e.g. start, stop, join, etc.
 * 
 * @author emil.rakadjiev
 *
 */
public interface ICluster {

	/**
	 * Starts this cluster
	 */
	public void start();
	
	/**
	 * Joins this cluster as a master node
	 */
	public void joinAsMaster();
	
	/**
	 * Joins this cluster as a slave node
	 */
	public void joinAsSlave();
	
	/**
	 * Stops this cluster
	 */
	public void stop();
	
	/**
	 * Generates an id for a slave node
	 * @return The id of the slave
	 */
	public int generateSlaveId();
	
	/**
	 * Generates an id for a master node
	 * @return The id of the master
	 */
	public int generateMasterId();
	
	/**
	 * Indicates that a master has finished the join process. Used to determine the number of nodes already in the cluster.
	 */
	public void masterJoined();
	
	
	/**
	 * Indicates that a slave has finished the join process. Used to determine the number of nodes already in the cluster.
	 */
	public void slaveJoined();
	
	/**
	 * Checks, whether all expected members have joined this cluster
	 * 
	 * @param masters Number of masters that have to join this cluster
	 * @param slaves Number of slaves that have to join this cluster
	 * @return True if all expected members have joined this cluster
	 */
	public boolean allMembersJoined(int masters, int slaves);
	
}
