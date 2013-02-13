package edu.cmu.pdl.metadatabench.cluster;

public interface ICluster {

	public void start();
	public void joinAsMaster();
	public void joinAsSlave();
	public void stop();
	public int generateSlaveId();
	public int generateMasterId();
	public boolean allMembersJoined(int masters, int slaves);
	
}
