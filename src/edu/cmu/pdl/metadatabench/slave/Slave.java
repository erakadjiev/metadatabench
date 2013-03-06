package edu.cmu.pdl.metadatabench.slave;

import java.util.Properties;

import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.measurement.Measurements;
import edu.cmu.pdl.metadatabench.measurement.OneMeasurementHistogram;
import edu.cmu.pdl.metadatabench.slave.fs.HDFSClient;
import edu.cmu.pdl.metadatabench.slave.progress.ProgressReporter;

public class Slave{

	private static final int THREADS = Config.getSlaveThreadPoolSize();
	private static final long REPORT_FREQUENCY = Config.getSlaveProgressReportFrequencyMillis();
	private static final String MEASUREMENT_WARM_UP_TIME = String.valueOf(Config.getMeasurementWarmUpTime());
	private static OperationExecutor executor;
	private static OperationHandler handler;
	
	public static void start(HazelcastInstance hazelcast, int id, String fileSystemAddress) {
		Properties props = new Properties();
		props.setProperty(Measurements.MEASUREMENT_TYPE, "histogram");
		props.setProperty(Measurements.MEASUREMENT_WARM_UP, MEASUREMENT_WARM_UP_TIME);
		props.setProperty(OneMeasurementHistogram.BUCKETS, String.valueOf(Config.getMeasurementHistogramBuckets()));
//		props.setProperty(OneMeasurementTimeSeries.GRANULARITY, String.valueOf(Config.getMeasurementTimeSeriesGranularity()));
		props.setProperty("nodeId", Integer.toString(id));
		Measurements.setProperties(props);
		
		executor = new OperationExecutor(new HDFSClient(fileSystemAddress), THREADS);
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
