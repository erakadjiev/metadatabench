package edu.cmu.pdl.metadatabench.slave;

import java.util.Properties;

import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.measurement.Measurements;
import edu.cmu.pdl.metadatabench.measurement.OneMeasurementHistogram;
import edu.cmu.pdl.metadatabench.slave.fs.HDFSClient;

public class Slave{

	private static final int THREADS = 100; // TODO: param
	private static final long REPORT_FREQUENCY = 2500;
	private static final String MEASUREMENT_WARM_UP_TIME = "20000";
	private static OperationExecutor executor;
	private static OperationHandler handler;
	
	public static void start(HazelcastInstance hazelcast, int id) {
		Properties props = new Properties();
		props.setProperty(Measurements.MEASUREMENT_TYPE, "histogram");
		props.setProperty(Measurements.MEASUREMENT_WARM_UP, MEASUREMENT_WARM_UP_TIME);
		props.setProperty(OneMeasurementHistogram.BUCKETS, "100");
//		props.setProperty(OneMeasurementTimeSeries.GRANULARITY, "1000");
		props.setProperty("nodeId", Integer.toString(id));
		Measurements.setProperties(props);
		
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
