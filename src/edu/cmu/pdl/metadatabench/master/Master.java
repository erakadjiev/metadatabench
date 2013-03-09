package edu.cmu.pdl.metadatabench.master;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

import edu.cmu.pdl.metadatabench.cluster.HazelcastMapDAO;
import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.HazelcastDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsCollect;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReset;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractDirectoryCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.AbstractFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.BarabasiAlbertDirectoryCreationStrategy;
import edu.cmu.pdl.metadatabench.master.namespace.NamespaceGenerator;
import edu.cmu.pdl.metadatabench.master.namespace.ZipfianFileCreationStrategy;
import edu.cmu.pdl.metadatabench.master.progress.ProgressMonitor;
import edu.cmu.pdl.metadatabench.master.workload.WorkloadGenerator;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataCollection;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

/**
 * Starts and coordinates the namespace and workload generation.
 * 
 * Launches the directory, file and workload generation, waits for each phase to complete, 
 * collects and outputs measurements.
 * 
 * @author emil.rakadjiev
 *
 */
public class Master {

	private static final int SLEEP_AFTER_GENERATION_STEP_MILLIS = 2500;

	/** Used for the timestamp in the measurement file names */
	private static final SimpleDateFormat DATE_FORMAT_FOR_LOG = new SimpleDateFormat("yyyyMMdd-HHmmss");
	// TODO: get log directory as external parameter
	/** Directory where the measurement files will be saved */
	private static final String LOG_DIRECTORY = "log/";
	
	private static Logger log = LoggerFactory.getLogger(Master.class);

	/**
	 * Launches the directory, file and workload generation, waits for each phase to complete, 
	 * collects and outputs measurements.
	 * 
	 * @param hazelcast The Hazelcast instance
	 * @param id Id of the master
	 * @param numberOfDirs Number of directories to generate
	 * @param numberOfFiles Number of files to generate
	 * @param numberOfOperations Number of operations to generate
	 */
	public static void start(HazelcastInstance hazelcast, int id, int numberOfDirs, int numberOfFiles, int numberOfOperations){
		
		INamespaceMapDAO dao = new HazelcastMapDAO(hazelcast);
		IDispatcher dispatcher = new HazelcastDispatcher(hazelcast);
		
		AbstractDirectoryCreationStrategy dirCreator = new BarabasiAlbertDirectoryCreationStrategy(dao, dispatcher);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dispatcher, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, id);
		
		// contains general measurements like runtime and throughput 
		Map<String,Double> overallMeasurements = new LinkedHashMap<String,Double>();
		
		if(numberOfDirs > 0){
			log.info("Dir creation started");
			long start = System.currentTimeMillis();
			// launch directory generation
			nsGen.generateDirs(numberOfDirs);
			long end = System.currentTimeMillis();
			log.info("{} dirs generated in: {}", numberOfDirs, (end-start)/1000.0);
			// wait for all generated operations to be executed 
			try {
				ProgressMonitor.awaitOperationCompletion(numberOfDirs);
			} catch (InterruptedException e) {
				log.error("Exception while waiting for all the directory creation operations to complete", e);
			}
			double creationTime = (System.currentTimeMillis()-start)/1000.0;
			double throughput = numberOfDirs/creationTime;
			log.info("Dir creation done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", throughput);
			overallMeasurements.put("Directory creation runtime (s)", creationTime);
			overallMeasurements.put("Directory creation throughput (ops/s)", throughput);
			log.debug("Resetting local and remote progress");
			// all operations executed in current phase, reset progress locally and on all slaves
			ProgressMonitor.reset();
			dispatcher.dispatch(new ProgressReset());
		}
		
		if(numberOfFiles > 0){
			// wait before starting the next phase
			try {
				log.debug("Going to sleep for {} seconds after directory creation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}

			log.info("File creation started");
			long start = System.currentTimeMillis();
			// launch file generation
			nsGen.generateFiles(numberOfFiles);
			long end = System.currentTimeMillis();
			log.info("{} files generated in: {}", numberOfFiles, (end-start)/1000.0);
			// wait for all generated operations to be executed 
			try {
				ProgressMonitor.awaitOperationCompletion(numberOfFiles);
			} catch (InterruptedException e) {
				log.error("Exception while waiting for all the file creation operations to complete", e);
			}
			double creationTime = (System.currentTimeMillis()-start)/1000.0;
			double throughput = numberOfFiles/creationTime;
			log.info("File creation done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", throughput);
			overallMeasurements.put("File creation runtime (s)", creationTime);
			overallMeasurements.put("File creation throughput (ops/s)", throughput);
			log.debug("Resetting local and remote progress");
			// all operations executed in current phase, reset progress locally and on all slaves
			ProgressMonitor.reset();
			dispatcher.dispatch(new ProgressReset());
		}
		
		// collect namespace creation measurements from slaves and export them 
		collectExportAndResetMeasurements(dispatcher, "namespace", overallMeasurements);
		
		if(numberOfOperations > 0){
			// reset overall measurements before starting the workload generation
			overallMeasurements = new LinkedHashMap<String,Double>();
			// wait before starting the next phase
			try {
				log.debug("Going to sleep for {} seconds after namespace generation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}

			log.info("Workload generation started");
			long start = System.currentTimeMillis();
			// launch workload generation
			WorkloadGenerator wlGen = new WorkloadGenerator(dispatcher, numberOfOperations, numberOfDirs, numberOfFiles);
			wlGen.generate();
			long end = System.currentTimeMillis();
			log.info("{} operations generated in: {}", numberOfOperations, (end-start)/1000.0);
			// wait for all generated operations to be executed 
			try {
				ProgressMonitor.awaitOperationCompletion(numberOfOperations);
			} catch (InterruptedException e) {
				log.error("Exception while waiting for all the workload operations to complete", e);
			}
			double creationTime = (System.currentTimeMillis()-start)/1000.0;
			double throughput = numberOfOperations/creationTime;
			log.info("Workload done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", throughput);
			overallMeasurements.put("Workload total operations", (double)numberOfOperations);
			overallMeasurements.put("Workload runtime (s)", creationTime);
			overallMeasurements.put("Workload throughput (ops/s)", throughput);
			log.debug("Resetting local and remote progress");
			// all operations executed in current phase, reset progress locally and on all slaves
			ProgressMonitor.reset();
			dispatcher.dispatch(new ProgressReset());
			// collect workload measurements from slaves and export them 
			collectExportAndResetMeasurements(dispatcher, "workload", overallMeasurements);
		}
	}
	
	/**
	 * Collects measurements from slaves, exports the combined measurements and resets the local and remote 
	 * measurement data
	 * 
	 * @param dispatcher The dispatcher used to send messages to other nodes
	 * @param generationStepName Name of the generation phase (namespace or workload)
	 * @param overallMeasurements The overall measurements (runtime, throughput)
	 */
	private static void collectExportAndResetMeasurements(IDispatcher dispatcher, String generationStepName, Map<String,Double> overallMeasurements){
		MeasurementDataCollection measurements = collectMeasurements(dispatcher);
		String measurementString = getAndExportMeasurementText(measurements, overallMeasurements);
		exportMeasurementTextToFile(measurementString, generationStepName);
		log.debug("Resetting local and remote measurements");
		dispatcher.dispatch(new MeasurementsReset());
		measurements.reset();
	}

	/**
	 * Collects measurements from slaves
	 * 
	 * @param dispatcher The dispatcher used to send messages to other nodes
	 * @return The combined measurements from all slaves
	 */
	private static MeasurementDataCollection collectMeasurements(IDispatcher dispatcher){
		MeasurementDataCollection measurements = MeasurementDataCollection.getInstance();
		try {
			Collection<MeasurementDataForNode> measurementDataCollection = dispatcher.dispatch(new MeasurementsCollect());
			for(MeasurementDataForNode measurementData : measurementDataCollection){
				measurements.addMeasurementData(measurementData);
			}
		} catch (Exception e) {
			log.error("Exception while collecting measurements from nodes", e);
		}
		return measurements;
	}
	
	/**
	 * Exports measurements in a text format
	 * 
	 * @param measurements The combined measurements
	 * @param overallMeasurements The overall measurements (runtime, throughput)
	 * @return The exported measurements (in a text format)
	 */
	private static String getAndExportMeasurementText(MeasurementDataCollection measurements, Map<String,Double> overallMeasurements){
		String measurementString = "";
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextMeasurementsExporter exporter = new TextMeasurementsExporter(baos);
			// export overall measurements
			String OVERALL = "OVERALL";
			Iterator<String> iterator = overallMeasurements.keySet().iterator();
			while(iterator.hasNext()){
				String measurement = iterator.next();
				double value = overallMeasurements.get(measurement);
				exporter.write(OVERALL, measurement, value);
			}
			// export detailed measurements
			measurements.exportMeasurements(exporter);
//			measurements.exportMeasurementsPerNode(exporter);
			exporter.close();
			measurementString = baos.toString();
			log.info(measurementString);
		} catch (IOException e) {
			log.warn("Exception while exporting measurements to a text format", e);
		}
		return measurementString;
	}
	
	/**
	 * Writes the exported measurement text into a file 
	 * 
	 * @param measurementString The exported measurements (in a text format)
	 * @param generationStepName Name of the generation phase (namespace or workload)
	 */
	private static void exportMeasurementTextToFile(String measurementString, String generationStepName){
		if((measurementString != null) && (!measurementString.isEmpty())){
			StringBuilder fileName = new StringBuilder();
			fileName.append(LOG_DIRECTORY);
			fileName.append(generationStepName);
			fileName.append("-");
			fileName.append(DATE_FORMAT_FOR_LOG.format(new Date()));
			fileName.append(".txt");
			FileWriter fileWriter = null;
			try {
				fileWriter = new FileWriter(fileName.toString());
				fileWriter.write(measurementString);
			} catch (IOException e) {
				log.warn("Exception while saving measurements to a file", e);
			} finally {
				if(fileWriter != null){
					try {
						fileWriter.close();
					} catch (IOException e) {
						log.warn("Exception while exporting measurements to a text format. Cannot close stream to file.", e);
					}
				}
			}
		}
	}
	
}
