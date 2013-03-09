package edu.cmu.pdl.metadatabench.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores the benchmark-wide configuration properties. Defines defaults that can be overridden by loading a 
 * config properties file using {@link edu.cmu.pdl.metadatabench.common.ConfigLoader}.
 * 
 * @author emil.rakadjiev
 *
 */
public class Config {

	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfSlaves() */
	private static int numberOfSlaves = -1;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfDirs() */
	private static int numberOfDirs = -1;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfFiles() */
	private static int numberOfFiles = -1;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfOps() */
	private static int numberOfOps = -1;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileSystemAddress() */
	private static String fileSystemAddress = null;
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getPathSeparator() */
	private static char pathSeparator = '/';
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkDir() */
	private static String workDir = "/workDir";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getDirNamePrefix() */
	private static String dirNamePrefix = "dir";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileNamePrefix() */
	private static String fileNamePrefix = "file";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadRenameSuffix() */
	private static String workloadRenameSuffix = ".r";
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadOperationProbabilities() */
	private static Map<FileSystemOperationType,Double> workloadOperationProbabilities = new HashMap<FileSystemOperationType,Double>();
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize() */
	private static int workloadAccessedElementCacheMaxSize = 110000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheTTL() */
	private static long workloadAccessedElementCacheTTL = 5000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps() */
	private static int workloadThrottleAfterGeneratedOps = 100000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() */
	private static int workloadThrottleContinueThreshold = 2000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleDuration() */
	private static int workloadThrottleDuration = 900;
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementWarmUpTime() */
	private static int measurementWarmUpTime = 0;
	/** @see edu.cmu.pdl.metadatabench.common.Config#isMeasurementHistogram() */
	private static boolean measurementHistogram = true;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementHistogramBuckets() */
	private static int measurementHistogramBuckets = 1000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementTimeSeriesGranularity() */
	private static int measurementTimeSeriesGranularity = 1000;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveThreadPoolSize() */
	private static int slaveThreadPoolSize = 100;
	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveProgressReportFrequencyMillis() */
	private static int slaveProgressReportFrequencyMillis = 2500;

	/* Set the default operation probabilities */
	static{
		workloadOperationProbabilities.put(FileSystemOperationType.CREATE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.DELETE_FILE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.LIST_STATUS_FILE, 0.4);
		workloadOperationProbabilities.put(FileSystemOperationType.LIST_STATUS_DIR, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.OPEN_FILE, 0.35);
		workloadOperationProbabilities.put(FileSystemOperationType.RENAME_FILE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.MOVE_FILE, 0.05);
	}

	/** Number of slave nodes in the cluster. Used to know when all nodes have joined and the generation can be started. */
	public static int getNumberOfSlaves() {
		return numberOfSlaves;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfSlaves() */
	public static void setNumberOfSlaves(int numberOfSlaves) {
		Config.numberOfSlaves = numberOfSlaves;
	}

	/** Number of directories to create during the initial namespace generation phase  */
	public static int getNumberOfDirs() {
		return numberOfDirs;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfDirs() */
	public static void setNumberOfDirs(int numberOfDirs) {
		Config.numberOfDirs = numberOfDirs;
	}

	/** Number of files to create during the initial namespace generation phase */
	public static int getNumberOfFiles() {
		return numberOfFiles;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfFiles() */
	public static void setNumberOfFiles(int numberOfFiles) {
		Config.numberOfFiles = numberOfFiles;
	}

	/** 
	 * Number of operations to execute during the workload phase. The mix of operations 
	 * can be specified with the operation probability parameters 
	 */
	public static int getNumberOfOps() {
		return numberOfOps;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfOps() */
	public static void setNumberOfOps(int numberOfOps) {
		Config.numberOfOps = numberOfOps;
	}

	/**
	 * Address of the file system server. For example for HDFS this is the value of the 
	 * fs.default.name or fs.defaultFS parameter as defined in core-site.xml, using the 
	 * format hdfs://host.name:port/. 
	 */
	public static String getFileSystemAddress() {
		return fileSystemAddress;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileSystemAddress() */
	public static void setFileSystemAddress(String fileSystemAddress) {
		Config.fileSystemAddress = fileSystemAddress;
	}

	/** The path separator to be used for the file system */
	public static char getPathSeparator() {
		return pathSeparator;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getPathSeparator() */
	public static void setPathSeparator(char pathSeparator) {
		Config.pathSeparator = pathSeparator;
	}
	
	/** The directory where the benchmark will operate (basically the root directory for the benchmark namespace */
	public static String getWorkDir() {
		return workDir;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkDir() */
	public static void setWorkDir(String workDir) {
		Config.workDir = workDir;
	}

	/** The prefix to use for directory names. After this prefix the id will be appended. For example dir69 */
	public static String getDirNamePrefix() {
		return dirNamePrefix;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getDirNamePrefix() */
	public static void setDirNamePrefix(String dirNamePrefix) {
		Config.dirNamePrefix = dirNamePrefix;
	}

	/** The prefix to use for file names. After this prefix the id will be appended. For example file20 */
	public static String getFileNamePrefix() {
		return fileNamePrefix;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileNamePrefix() */
	public static void setFileNamePrefix(String fileNamePrefix) {
		Config.fileNamePrefix = fileNamePrefix;
	}
	
	/** 
	 * When an element is renamed, this suffix and a rename count is appended to or incremented at the end of its name. 
	 * For example file20 -> file20.r1 or file20.r69 -> file20.r70
	 */
	public static String getWorkloadRenameSuffix() {
		return workloadRenameSuffix;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadRenameSuffix() */
	public static void setWorkloadRenameSuffix(String workloadRenameSuffix) {
		Config.workloadRenameSuffix = workloadRenameSuffix;
	}

	/**
	 * Map of operation types and their probability. An operation probability is the percentage of operations of the 
	 * given type in the workload ({@link edu.cmu.pdl.metadatabench.common.Config#getNumberOfOps()}). The values 
	 * have to be between 0 (exclusive) and 1 (inclusive) and have to add up to 1.
	 * If an operation is not needed in the workload, exclude it, instead of setting it to 0. 
	 */
	public static Map<FileSystemOperationType, Double> getWorkloadOperationProbabilities() {
		return workloadOperationProbabilities;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadOperationProbabilities() */
	public static void setWorkloadOperationProbabilities(
			Map<FileSystemOperationType, Double> workloadOperationProbabilities) {
		Config.workloadOperationProbabilities = workloadOperationProbabilities;
	}

	/**
	 * During the workload execution, accessed elements are cached for a given time, in order to prevent further 
	 * access to them that could cause conflicts. The reason is that the benchmark is distributed and operates 
	 * asynchronously and thus the workload operation generation is not the same as the execution order. For example 
	 * an open file20 operation can be generated, followed by a delete file20. Both are dispatched and the delete 
	 * operation could get executed before the open, leading to an error.
	 * This parameter specifies the maximum size of the cache.
	 * If throttling is needed for your workload, setting the maximum size of the cache to a value slightly 
	 * higher than {@link edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps()} 
	 * and adjusting the {@link edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheTTL()} 
	 * parameter can minimize or prevent file system access conflicts.
	 * Note that this cache is local to the workload generator (in the master).
	 */
	public static int getWorkloadAccessedElementCacheMaxSize() {
		return workloadAccessedElementCacheMaxSize;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize() */
	public static void setWorkloadAccessedElementCacheMaxSize(
			int workloadAccessedElementCacheMaxSize) {
		Config.workloadAccessedElementCacheMaxSize = workloadAccessedElementCacheMaxSize;
	}

	/**
	 * This parameter specifies the time that has to pass between generating two operations that access the same 
	 * directory or file.
	 * @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize() 
	 */
	public static long getWorkloadAccessedElementCacheTTL() {
		return workloadAccessedElementCacheTTL;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheTTL() */
	public static void setWorkloadAccessedElementCacheTTL(
			long workloadAccessedElementCacheTTL) {
		Config.workloadAccessedElementCacheTTL = workloadAccessedElementCacheTTL;
	}

	/** 
	 * If the generator is faster than the slaves or the file system under test, then unexecuted operations 
	 * could queue up and slow the down the benchmark. Thus, this option is provided to throttle the generation 
	 * and prevent such an overload of the distributed system.
	 * This parameter specifies after how many generated operations, the benchmark should check whether the slaves 
	 * could process a sufficient amount of the generated operations. For example, if you set this parameter to 
	 * 100000, then after every 100000th generated operation, the master will check how many operations have been 
	 * executed by the slaves and if needed, it will sleep until a given threshold has been reached.
	 * @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() 
	 */
	public static int getWorkloadThrottleAfterGeneratedOps() {
		return workloadThrottleAfterGeneratedOps;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps() */
	public static void setWorkloadThrottleAfterGeneratedOps(
			int workloadThrottleAfterGeneratedOps) {
		Config.workloadThrottleAfterGeneratedOps = workloadThrottleAfterGeneratedOps;
	}

	/** 
	 * Specifies the maximum difference between generated and executed operations that has to be reached 
	 * before continuing with the generation. For example if this parameter is set to 2000 and 
	 * {@link edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps()} is 100000, 
	 * then the master will wait until at least 98000 operations have been executed before generating new operations. 
	 */
	public static int getWorkloadThrottleContinueThreshold() {
		return workloadThrottleContinueThreshold;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() */
	public static void setWorkloadThrottleContinueThreshold(
			int workloadThrottleContinueThreshold) {
		Config.workloadThrottleContinueThreshold = workloadThrottleContinueThreshold;
	}

	/** 
	 * The time to sleep between two checks whether the needed amount of operations have been executed by the slaves.
	 * @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps() 
	 * @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() 
	 */
	public static int getWorkloadThrottleDuration() {
		return workloadThrottleDuration;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleDuration() */
	public static void setWorkloadThrottleDuration(int workloadThrottleDuration) {
		Config.workloadThrottleDuration = workloadThrottleDuration;
	}

	/** The warm-up time of the system, that is, the time to wait between starting the operation execution and starting the measurements. */
	public static int getMeasurementWarmUpTime() {
		return measurementWarmUpTime;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementWarmUpTime() */
	public static void setMeasurementWarmUpTime(int measurementWarmUpTime) {
		Config.measurementWarmUpTime = measurementWarmUpTime;
	}

	/**
	 * If true, the measurement type will be histogram, otherwise it will be time series.
	 * @see edu.cmu.pdl.metadatabench.measurement.Measurements
	 */
	public static boolean isMeasurementHistogram() {
		return measurementHistogram;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#isMeasurementHistogram() */
	public static void setMeasurementHistogram(boolean measurementHistogram) {
		Config.measurementHistogram = measurementHistogram;
	}

	/**
	 * The number of buckets for the measurement histogram. Mutually exclusive with 
	 * {@link edu.cmu.pdl.metadatabench.common.Config#getMeasurementTimeSeriesGranularity()}.
	 * @see edu.cmu.pdl.metadatabench.measurement.Measurements
	 * @see edu.cmu.pdl.metadatabench.measurement.OneMeasurementHistogram
	 */
	public static int getMeasurementHistogramBuckets() {
		return measurementHistogramBuckets;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementHistogramBuckets() */
	public static void setMeasurementHistogramBuckets(
			int measurementHistogramBuckets) {
		Config.measurementHistogramBuckets = measurementHistogramBuckets;
	}

	/**
	 * The time step for the time series measurement. Mutually exclusive with 
	 * {@link edu.cmu.pdl.metadatabench.common.Config#getMeasurementHistogramBuckets()}.
	 * @see edu.cmu.pdl.metadatabench.measurement.Measurements
	 * @see edu.cmu.pdl.metadatabench.measurement.OneMeasurementTimeSeries
	 */
	public static int getMeasurementTimeSeriesGranularity() {
		return measurementTimeSeriesGranularity;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementTimeSeriesGranularity() */
	public static void setMeasurementTimeSeriesGranularity(
			int measurementTimeSeriesGranularity) {
		Config.measurementTimeSeriesGranularity = measurementTimeSeriesGranularity;
	}

	/** The size of the thread pool used for file system operation execution at each slave. */
	public static int getSlaveThreadPoolSize() {
		return slaveThreadPoolSize;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveThreadPoolSize() */
	public static void setSlaveThreadPoolSize(int slaveThreadPoolSize) {
		Config.slaveThreadPoolSize = slaveThreadPoolSize;
	}

	/** The frequency (in milliseconds) with which each slave should report its progress (number of executed operations) to the master. */
	public static int getSlaveProgressReportFrequencyMillis() {
		return slaveProgressReportFrequencyMillis;
	}

	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveProgressReportFrequencyMillis() */
	public static void setSlaveProgressReportFrequencyMillis(
			int slaveProgressReportFrequencyMillis) {
		Config.slaveProgressReportFrequencyMillis = slaveProgressReportFrequencyMillis;
	}
	
}
