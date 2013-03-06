package edu.cmu.pdl.metadatabench.common;

import java.util.HashMap;
import java.util.Map;

public class Config {

	private static int numberOfSlaves = -1;
	private static int numberOfDirs = -1;
	private static int numberOfFiles = -1;
	private static int numberOfOps = -1;
	private static String fileSystemAddress = null;
	
	private static char pathSeparator = '/';
	private static String workingDir = "/workDir";
	private static String dirNamePrefix = "dir";
	private static String fileNamePrefix = "file";
	private static String workloadRenameSuffix = ".r";
	private static Map<FileSystemOperationType,Double> workloadOperationProbabilities = new HashMap<FileSystemOperationType,Double>();
	private static int workloadAccessedElementCacheMaxSize = 110000;
	private static long workloadAccessedElementCacheTTL = 5000;
	private static int workloadThrottleAfterGeneratedOps = 100000;
	private static int workloadThrottleContinueThreshold = 2000;
	private static int workloadThrottleDuration = 900;
	private static int measurementWarmUpTime = 0;
	private static int measurementHistogramBuckets = 1000;
	private static int measurementTimeSeriesGranularity = 1000;
	private static int slaveThreadPoolSize = 100;
	private static int slaveProgressReportFrequencyMillis = 2500;

	static{
		workloadOperationProbabilities.put(FileSystemOperationType.CREATE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.DELETE_FILE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.LIST_STATUS_FILE, 0.4);
		workloadOperationProbabilities.put(FileSystemOperationType.LIST_STATUS_DIR, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.OPEN_FILE, 0.35);
		workloadOperationProbabilities.put(FileSystemOperationType.RENAME_FILE, 0.05);
		workloadOperationProbabilities.put(FileSystemOperationType.MOVE_FILE, 0.05);
	}

	public static int getNumberOfSlaves() {
		return numberOfSlaves;
	}

	public static void setNumberOfSlaves(int numberOfSlaves) {
		Config.numberOfSlaves = numberOfSlaves;
	}

	public static int getNumberOfDirs() {
		return numberOfDirs;
	}

	public static void setNumberOfDirs(int numberOfDirs) {
		Config.numberOfDirs = numberOfDirs;
	}

	public static int getNumberOfFiles() {
		return numberOfFiles;
	}

	public static void setNumberOfFiles(int numberOfFiles) {
		Config.numberOfFiles = numberOfFiles;
	}

	public static int getNumberOfOps() {
		return numberOfOps;
	}

	public static void setNumberOfOps(int numberOfOps) {
		Config.numberOfOps = numberOfOps;
	}

	public static String getFileSystemAddress() {
		return fileSystemAddress;
	}

	public static void setFileSystemAddress(String fileSystemAddress) {
		Config.fileSystemAddress = fileSystemAddress;
	}

	public static char getPathSeparator() {
		return pathSeparator;
	}

	public static void setPathSeparator(char pathSeparator) {
		Config.pathSeparator = pathSeparator;
	}
	
	public static String getWorkingDir() {
		return workingDir;
	}

	public static void setWorkingDir(String workDir) {
		Config.workingDir = workDir;
	}

	public static String getDirNamePrefix() {
		return dirNamePrefix;
	}

	public static void setDirNamePrefix(String dirNamePrefix) {
		Config.dirNamePrefix = dirNamePrefix;
	}

	public static String getFileNamePrefix() {
		return fileNamePrefix;
	}

	public static void setFileNamePrefix(String fileNamePrefix) {
		Config.fileNamePrefix = fileNamePrefix;
	}
	
	public static String getWorkloadRenameSuffix() {
		return workloadRenameSuffix;
	}

	public static void setWorkloadRenameSuffix(String workloadRenameSuffix) {
		Config.workloadRenameSuffix = workloadRenameSuffix;
	}

	public static Map<FileSystemOperationType, Double> getWorkloadOperationProbabilities() {
		return workloadOperationProbabilities;
	}

	public static void setWorkloadOperationProbabilities(
			Map<FileSystemOperationType, Double> workloadOperationProbabilities) {
		Config.workloadOperationProbabilities = workloadOperationProbabilities;
	}

	public static int getWorkloadAccessedElementCacheMaxSize() {
		return workloadAccessedElementCacheMaxSize;
	}

	public static void setWorkloadAccessedElementCacheMaxSize(
			int workloadAccessedElementCacheMaxSize) {
		Config.workloadAccessedElementCacheMaxSize = workloadAccessedElementCacheMaxSize;
	}

	public static long getWorkloadAccessedElementCacheTTL() {
		return workloadAccessedElementCacheTTL;
	}

	public static void setWorkloadAccessedElementCacheTTL(
			long workloadAccessedElementCacheTTL) {
		Config.workloadAccessedElementCacheTTL = workloadAccessedElementCacheTTL;
	}

	public static int getWorkloadThrottleAfterGeneratedOps() {
		return workloadThrottleAfterGeneratedOps;
	}

	public static void setWorkloadThrottleAfterGeneratedOps(
			int workloadThrottleAfterGeneratedOps) {
		Config.workloadThrottleAfterGeneratedOps = workloadThrottleAfterGeneratedOps;
	}

	public static int getWorkloadThrottleContinueThreshold() {
		return workloadThrottleContinueThreshold;
	}

	public static void setWorkloadThrottleContinueThreshold(
			int workloadThrottleContinueThreshold) {
		Config.workloadThrottleContinueThreshold = workloadThrottleContinueThreshold;
	}

	public static int getWorkloadThrottleDuration() {
		return workloadThrottleDuration;
	}

	public static void setWorkloadThrottleDuration(int workloadThrottleDuration) {
		Config.workloadThrottleDuration = workloadThrottleDuration;
	}

	public static int getMeasurementWarmUpTime() {
		return measurementWarmUpTime;
	}

	public static void setMeasurementWarmUpTime(int measurementWarmUpTime) {
		Config.measurementWarmUpTime = measurementWarmUpTime;
	}

	public static int getMeasurementHistogramBuckets() {
		return measurementHistogramBuckets;
	}

	public static void setMeasurementHistogramBuckets(
			int measurementHistogramBuckets) {
		Config.measurementHistogramBuckets = measurementHistogramBuckets;
	}

	public static int getMeasurementTimeSeriesGranularity() {
		return measurementTimeSeriesGranularity;
	}

	public static void setMeasurementTimeSeriesGranularity(
			int measurementTimeSeriesGranularity) {
		Config.measurementTimeSeriesGranularity = measurementTimeSeriesGranularity;
	}

	public static int getSlaveThreadPoolSize() {
		return slaveThreadPoolSize;
	}

	public static void setSlaveThreadPoolSize(int slaveThreadPoolSize) {
		Config.slaveThreadPoolSize = slaveThreadPoolSize;
	}

	public static int getSlaveProgressReportFrequencyMillis() {
		return slaveProgressReportFrequencyMillis;
	}

	public static void setSlaveProgressReportFrequencyMillis(
			int slaveProgressReportFrequencyMillis) {
		Config.slaveProgressReportFrequencyMillis = slaveProgressReportFrequencyMillis;
	}
	
}
