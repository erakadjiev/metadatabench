package edu.cmu.pdl.metadatabench.slave;

import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.slave.fs.HDFSClient;

public class Slave{

	private static final int THREADS = 100; // TODO: param
	private static final long REPORT_FREQUENCY = 2500;
	private static OperationExecutor executor;
	private static OperationHandler handler;
	
	public static void start(HazelcastInstance hazelcast, int id) {
		executor = new OperationExecutor(new HDFSClient(), THREADS);
		handler = new OperationHandler(executor, new HazelcastMapDAO(hazelcast));
		
		new Thread(new ProgressReporter(id, new HazelcastDispatcher(hazelcast), REPORT_FREQUENCY)).start();
	}
	
	public static OperationHandler getOperationHandler(){
		return handler;
	}
	
	public static void shutdown(){
		ProgressReporter.stop();
		executor.shutdown();
	}

}
