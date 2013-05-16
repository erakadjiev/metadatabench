package edu.cmu.pdl.metadatabench.slave;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;

import edu.cmu.pdl.metadatabench.cluster.HazelcastCluster;
import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.measurement.Measurements;
import edu.cmu.pdl.metadatabench.measurement.OneMeasurementHistogram;
import edu.cmu.pdl.metadatabench.measurement.OneMeasurementTimeSeries;
import edu.cmu.pdl.metadatabench.slave.fs.HDFSClient;
import edu.cmu.pdl.metadatabench.slave.progress.ProgressReporter;

/**
 * A slave receives operations from the master, executes them on the file system and measures the performance. It 
 * also reports its progress and the measurements to the master.
 * 
 * This class sets up the components of the slave node (the operation handler, the operation executor, 
 * the measurements and the progress reporter).
 * 
 * @author emil.rakadjiev
 *
 */
public class Slave{

	private static OperationExecutor executor;
	private static OperationHandler handler;
	
	private static Logger log = LoggerFactory.getLogger(Slave.class);
	
	/**
	 * Sets up the components of the slave node (the operation handler, the operation executor, 
	 * the measurements and the progress reporter).
	 * @param hazelcast The Hazelcast instance
	 * @param id The id of this node
	 * @param fileSystemAddress The address of the file system @see edu.cmu.pdl.metadatabench.common.Config#getFileSystemAddress()
	 */
	public static void start(HazelcastInstance hazelcast, int id, String fileSystemAddress) {
		Properties props = new Properties();
		String warmUp = String.valueOf(Config.getMeasurementWarmUpTime());
		props.setProperty(Measurements.MEASUREMENT_WARM_UP, warmUp);
		if(Config.isMeasurementHistogram()){
			props.setProperty(Measurements.MEASUREMENT_TYPE, Measurements.MEASUREMENT_TYPE_HISTOGRAM);
			props.setProperty(OneMeasurementHistogram.BUCKETS, String.valueOf(Config.getMeasurementHistogramBuckets()));
		} else {
			props.setProperty(Measurements.MEASUREMENT_TYPE, Measurements.MEASUREMENT_TYPE_TIMESERIES);
			props.setProperty(OneMeasurementTimeSeries.GRANULARITY, String.valueOf(Config.getMeasurementTimeSeriesGranularity()));
		}
		props.setProperty(Measurements.NODE_ID, Integer.toString(id));
		Measurements.setProperties(props);
		
		int threads = Config.getSlaveThreadPoolSize();
		executor = new OperationExecutor(new HDFSClient(fileSystemAddress), threads);
//		executor = new OperationExecutor(new DummyClient(), threads);
		handler = new OperationHandler(executor, new HazelcastMapDAO(hazelcast));
		
		long reportFrequency = Config.getSlaveProgressReportFrequencyMillis();
		
		new Thread(new ProgressReporter(id, new HazelcastDispatcher(hazelcast), reportFrequency)).start();
	}
	
	/**
	 * Gets the operation handler
	 * @return The operation handler
	 */
	public static OperationHandler getOperationHandler(){
		return handler;
	}
	
	/**
	 * Gets the operation executor
	 * @return The operation executor
	 */
	public static OperationExecutor getOperationExecutor(){
		return executor;
	}
	
	/**
	 * Shut down this node's components
	 */
	public static void shutdown(){
		log.info("Shutting down.");
		ProgressReporter.stop();
		executor.shutdown();
		HazelcastCluster.getInstance().stop();
	}

}
