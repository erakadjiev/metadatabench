package edu.cmu.pdl.metadatabench.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads a file containing configuration parameters for the benchmark. The parameters are parsed, verified 
 * and applied to the benchmark-wide {@link edu.cmu.pdl.metadatabench.common.Config configuration}.
 * 
 * @author emil.rakadjiev
 *
 */
public class ConfigLoader {

	private static Logger log = LoggerFactory.getLogger(ConfigLoader.class);

	/** Path where to look for a config properties file */
	private static final String CONFIG_PATH_DEFAULT = "metadatabench.properties";
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfSlaves() */
	private static final String NUMBER_OF_SLAVES = 							"master.numberofslaves";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfDirs() */
	private static final String NUMBER_OF_DIRS = 							"master.numberofdirs";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfFiles() */
	private static final String NUMBER_OF_FILES = 							"master.numberoffiles";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getNumberOfOps() */
	private static final String NUMBER_OF_OPS = 							"master.numberofops";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileSystemAddress() */
	private static final String FILE_SYSTEM_ADDRESS = 						"slave.filesystemaddress";
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getPathSeparator() */
	private static final String PATH_SEPARATOR = 							"misc.pathseparator";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkDir() */
	private static final String WORK_DIR = 								"master.namespace.workdir";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getDirNamePrefix() */
	private static final String DIR_NAME_PREFIX = 							"master.namespace.dirnameprefix";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getFileNamePrefix() */
	private static final String FILE_NAME_PREFIX = 							"master.namespace.filenameprefix";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadRenameSuffix() */
	private static final String WORKLOAD_RENAME_SUFFIX = 					"master.workload.renamesuffix";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheMaxSize() */
	private static final String WORKLOAD_ACCESSED_ELEMENT_CACHE_MAX_SIZE = 	"master.workload.accessedelementcache.maxsize";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadAccessedElementCacheTTL() */
	private static final String WORKLOAD_ACCESSED_ELEMENT_CACHE_TTL = 		"master.workload.accessedelementcache.ttl";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleAfterGeneratedOps() */
	private static final String WORKLOAD_THROTTLE_AFTER_GENERATED_OPS =		"master.workload.throttle.aftergeneratedops";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleContinueThreshold() */
	private static final String WORKLOAD_THROTTLE_CONTINUE_THRESHOLD =		"master.workload.throttle.continuethreshold";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadThrottleDuration() */
	private static final String WORKLOAD_THROTTLE_DURATION = 				"master.workload.throttle.duration";

	/** 
	 * The probability that a create file operation is generated, that is the percentage of 
	 * create operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_CREATE_PROBABILITY =				"master.workload.operation.create";
	/** 
	 * The probability that a mkdir operation is generated, that is the percentage of 
	 * mkdir operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_MKDIR_PROBABILITY =				"master.workload.operation.mkdir";
	/** 
	 * The probability that a delete file operation is generated, that is the percentage of 
	 * delete operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_DELETE_PROBABILITY =				"master.workload.operation.delete";
	/** 
	 * The probability that an ls file operation is generated, that is the percentage of 
	 * ls file operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_LSFILE_PROBABILITY =				"master.workload.operation.lsfile";
	/** 
	 * The probability that an ls directory operation is generated, that is the percentage of 
	 * ls directory operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_LSDIR_PROBABILITY =				"master.workload.operation.lsdir";
	/** 
	 * The probability that an open file operation is generated, that is the percentage of 
	 * open operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_OPENFILE_PROBABILITY =				"master.workload.operation.openfile";
	/** 
	 * The probability that a rename file operation is generated, that is the percentage of 
	 * rename operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_RENAMEFILE_PROBABILITY =			"master.workload.operation.renamefile";
	/** 
	 * The probability that a move file operation is generated, that is the percentage of 
	 * move operations in the workload ({@link edu.cmu.pdl.metadatabench.common.ConfigLoader#NUMBER_OF_OPS}). 
	 * The value has to be between 0 (exclusive) and 1 (inclusive).
	 */
	private static final String WORKLOAD_MOVEFILE_PROBABILITY =				"master.workload.operation.movefile";
	
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementWarmUpTime() */
	private static final String MEASUREMENT_WARMUP_TIME = 					"measurement.warmup";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementHistogramBuckets() */
	private static final String MEASUREMENT_HISTOGRAM_BUCKETS = 			"measurement.histogrambuckets";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementTimeSeriesGranularity() */
	private static final String MEASUREMENT_TIMESERIES_GRANULARITY = 		"measurement.timeseriesgranularity";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveThreadPoolSize() */
	private static final String SLAVE_THREADPOOL_SIZE = 					"slave.threadpoolsize";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getSlaveProgressReportFrequencyMillis() */
	private static final String SLAVE_PROGRESS_REPORT_FREQUENCY_MILLIS = 	"slave.progressreportfrequency";
	
	/**
	 * Loads the default config file. Looks for the file in both the classpath and the local file system 
	 * (relative to the JVM work directory). If there is no such file, leaves the default 
	 * settings as defined in {@link edu.cmu.pdl.metadatabench.common.Config}.
	 */
	public static void loadConfig(){
		loadConfig(CONFIG_PATH_DEFAULT);
	}
	
	/**
	 * Loads a config properties file from the specified path. Looks for the file in the local file system 
	 * (relative to the JVM work directory) and if it is not found, in the classpath. If there is no such file, 
	 * leaves the default settings as defined in {@link edu.cmu.pdl.metadatabench.common.Config}.
	 * @param configPath The path of the config file to load
	 */
	public static void loadConfig(String configPath){
		Properties config = new Properties();
		
		try {
			InputStream configIS = new FileInputStream(configPath);
			config.load(configIS);
		} catch (FileNotFoundException e) {
			InputStream configIS = ConfigLoader.class.getClassLoader().getResourceAsStream(configPath);
			if(configIS == null){
				log.warn("Config properties file not found, using default settings");
				return;
			} else {
				try {
					config.load(configIS);
				} catch (IOException e1) {
					log.warn("Cannot load config properties file, using default settings");
					return;
				}
			}
		} catch (IOException e) {
			log.warn("Cannot load config properties file, using default settings");
			return;
		}
		
		applyConfig(config);
	}

	/**
	 * Parses and verifies the specified config properties and applies the settings to the benchmark-wide 
	 * {@link edu.cmu.pdl.metadatabench.common.Config}
	 * @param config The config properties to apply
	 */
	private static void applyConfig(Properties config) {
		Map<FileSystemOperationType,Double> workloadOperationProbabilities = new HashMap<FileSystemOperationType,Double>();
		
		Set<String> props = config.stringPropertyNames();
		for(String prop : props){
			String value = config.getProperty(prop);
			if(value == null){
				log.warn("Value of property {} is null", prop);
			} else if(NUMBER_OF_SLAVES.equalsIgnoreCase(prop)){

				try{
					int slaves = Integer.parseInt(value);
					if(slaves < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setNumberOfSlaves(slaves);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			}   else if(NUMBER_OF_DIRS.equalsIgnoreCase(prop)){

				try{
					int dirs = Integer.parseInt(value);
					if(dirs < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setNumberOfDirs(dirs);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			}   else if(NUMBER_OF_FILES.equalsIgnoreCase(prop)){

				try{
					int files = Integer.parseInt(value);
					if(files < 0){
						log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setNumberOfFiles(files);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			}   else if(NUMBER_OF_OPS.equalsIgnoreCase(prop)){

				try{
					int ops = Integer.parseInt(value);
					if(ops < 0){
						log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setNumberOfOps(ops);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			}  else if(FILE_SYSTEM_ADDRESS.equalsIgnoreCase(prop)){
				
				log.debug("Set config parameter {} to {}", prop, value);
				Config.setFileSystemAddress(value);
				
			}  else if(PATH_SEPARATOR.equalsIgnoreCase(prop)){
				
				if(value.length() != 1){
					log.warn("Value of config parameter {} must be a single character", prop);
				} else {
					log.debug("Set config parameter {} to {}", prop, value);
					Config.setPathSeparator(value.charAt(0));
				}
				
			}  else if(WORK_DIR.equalsIgnoreCase(prop)){
				
				log.debug("Set config parameter {} to {}", prop, value);
				Config.setWorkDir(value);
				
			} else if(DIR_NAME_PREFIX.equalsIgnoreCase(prop)){
				
				log.debug("Set config parameter {} to {}", prop, value);
				Config.setDirNamePrefix(value);
				
			} else if(FILE_NAME_PREFIX.equalsIgnoreCase(prop)){

				log.debug("Set config parameter {} to {}", prop, value);
				Config.setFileNamePrefix(value);
				
			} else if(WORKLOAD_RENAME_SUFFIX.equalsIgnoreCase(prop)){
				
				log.debug("Set config parameter {} to {}", prop, value);
				Config.setWorkloadRenameSuffix(value);
				
			} else if(WORKLOAD_ACCESSED_ELEMENT_CACHE_MAX_SIZE.equalsIgnoreCase(prop)){

				try{
					int size = Integer.parseInt(value);
					if(size < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setWorkloadAccessedElementCacheMaxSize(size);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(WORKLOAD_ACCESSED_ELEMENT_CACHE_TTL.equalsIgnoreCase(prop)){
				
				try{
					int ttl = Integer.parseInt(value);
					if(ttl < 0){
						log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setWorkloadAccessedElementCacheTTL(ttl);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(WORKLOAD_THROTTLE_AFTER_GENERATED_OPS.equalsIgnoreCase(prop)){
				
				try{
					int ops = Integer.parseInt(value);
					if(ops < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setWorkloadThrottleAfterGeneratedOps(ops);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(WORKLOAD_THROTTLE_CONTINUE_THRESHOLD.equalsIgnoreCase(prop)){
				
				try{
					int threshold = Integer.parseInt(value);
					if(threshold < 0){
						log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setWorkloadThrottleContinueThreshold(threshold);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			}  else if(WORKLOAD_THROTTLE_DURATION.equalsIgnoreCase(prop)){
				
				try{
					int duration = Integer.parseInt(value);
					if(duration < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setWorkloadThrottleDuration(duration);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(WORKLOAD_CREATE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.CREATE, workloadOperationProbabilities);
				
			} else if(WORKLOAD_MKDIR_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.MKDIRS, workloadOperationProbabilities);
				
			} else if(WORKLOAD_DELETE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.DELETE_FILE, workloadOperationProbabilities);
				
			} else if(WORKLOAD_LSFILE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.LIST_STATUS_FILE, workloadOperationProbabilities);
				
			} else if(WORKLOAD_LSDIR_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.LIST_STATUS_DIR, workloadOperationProbabilities);
				
			} else if(WORKLOAD_OPENFILE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.OPEN_FILE, workloadOperationProbabilities);
				
			} else if(WORKLOAD_RENAMEFILE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.RENAME_FILE, workloadOperationProbabilities);
				
			} else if(WORKLOAD_MOVEFILE_PROBABILITY.equalsIgnoreCase(prop)){
				
				handleOperationProbabilityParameter(prop, value, FileSystemOperationType.MOVE_FILE, workloadOperationProbabilities);
				
			} else if(MEASUREMENT_WARMUP_TIME.equalsIgnoreCase(prop)){
				
				try{
					int time = Integer.parseInt(value);
					if(time < 0){
						log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setMeasurementWarmUpTime(time);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer or 0", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(MEASUREMENT_HISTOGRAM_BUCKETS.equalsIgnoreCase(prop)){
				
				if(config.containsKey(MEASUREMENT_TIMESERIES_GRANULARITY)){
					log.warn("{} and {} are mutually exclusive parameters. Will use {}", prop, MEASUREMENT_TIMESERIES_GRANULARITY, prop);
				}
				
				try{
					int buckets = Integer.parseInt(value);
					if(buckets < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setMeasurementHistogram(true);
						Config.setMeasurementHistogramBuckets(buckets);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(MEASUREMENT_TIMESERIES_GRANULARITY.equalsIgnoreCase(prop)){
				
				if(config.containsKey(MEASUREMENT_HISTOGRAM_BUCKETS)){
					log.warn("{} and {} are mutually exclusive parameters. Will use {}", prop, MEASUREMENT_HISTOGRAM_BUCKETS, MEASUREMENT_HISTOGRAM_BUCKETS);
				} else {
					try{
						int granularity = Integer.parseInt(value);
						if(granularity < 1){
							log.warn("Value for config parameter {} must be a positive integer", prop);
						} else {
							log.debug("Set config parameter {} to {}", prop, value);
							Config.setMeasurementHistogram(false);
							Config.setMeasurementTimeSeriesGranularity(granularity);
						}
					} catch(NumberFormatException e){
						log.warn("Value for config parameter {} must be a positive integer", prop);
						log.debug("Failed parsing config parameter value", e);
					}
				}
				
				
			} else if(SLAVE_THREADPOOL_SIZE.equalsIgnoreCase(prop)){
				
				try{
					int size = Integer.parseInt(value);
					if(size < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setSlaveThreadPoolSize(size);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else if(SLAVE_PROGRESS_REPORT_FREQUENCY_MILLIS.equalsIgnoreCase(prop)){
				
				try{
					int freq = Integer.parseInt(value);
					if(freq < 1){
						log.warn("Value for config parameter {} must be a positive integer", prop);
					} else {
						log.debug("Set config parameter {} to {}", prop, value);
						Config.setSlaveProgressReportFrequencyMillis(freq);
					}
				} catch(NumberFormatException e){
					log.warn("Value for config parameter {} must be a positive integer", prop);
					log.debug("Failed parsing config parameter value", e);
				}
				
			} else {
				log.warn("Unknown config parameter: {}", prop);
			}
		}
		
		if(getOperationProbabilitesSum(workloadOperationProbabilities) == 1){
			Config.setWorkloadOperationProbabilities(workloadOperationProbabilities);
		} else {
			log.warn("Supplied workload operation probabilities do not add up to 1. Using default operation probabilites.");
		}
		
	}
	
	/**
	 * Parses, verifies and saves an operation probability property.
	 * 
	 * @param prop The property key
	 * @param value The value of the property (that is the operation probability)
	 * @param type The operation type
	 * @param workloadOperationProbabilities The map containing the already saved operation probabilities
	 */
	private static void handleOperationProbabilityParameter(String prop, String value, FileSystemOperationType type, Map<FileSystemOperationType,Double> workloadOperationProbabilities){
		try{
			double probability = Double.parseDouble(value);
			if((probability <= 0) || (probability > 1)){
				log.warn("Value for config parameter {} must be between 0 (exclusive) and 1 (inclusive)", prop);
			} else {
				log.debug("Set config parameter {} to {}", prop, value);
				workloadOperationProbabilities.put(type, probability);
			}
		} catch(NumberFormatException e){
			log.warn("Value for config parameter {} must be between 0 and 1", prop);
			log.debug("Failed parsing config parameter value", e);
		}
	}
	
	/**
	 * Gets the sum of operation probabilities.
	 * 
	 * @param workloadOperationProbabilities The map containing the operation probabilities
	 * @return The sum of the operation probabilities
	 */
	private static double getOperationProbabilitesSum(Map<FileSystemOperationType,Double> workloadOperationProbabilities){
		Set<FileSystemOperationType> keys = workloadOperationProbabilities.keySet();
		double sum = 0;
		for(FileSystemOperationType key: keys){
			sum += workloadOperationProbabilities.get(key);
		}
		return sum;
	}

}
