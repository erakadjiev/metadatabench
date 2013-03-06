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
import edu.cmu.pdl.metadatabench.master.progress.ProgressBarrier;
import edu.cmu.pdl.metadatabench.master.workload.WorkloadGenerator;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataCollection;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

public class Master {

	private static final int SLEEP_AFTER_GENERATION_STEP_MILLIS = 2500;
	
	private static final SimpleDateFormat DATE_FORMAT_FOR_LOG = new SimpleDateFormat("yyyyMMdd-HHmmss");
	// TODO: get log directory as external parameter
	private static final String LOG_DIRECTORY = "log/";
	
	private static Logger log = LoggerFactory.getLogger(Master.class);

	public static void start(HazelcastInstance hazelcast, int id, int numberOfDirs, int numberOfFiles, int numberOfOperations){
		
		INamespaceMapDAO dao = new HazelcastMapDAO(hazelcast);
		IDispatcher dispatcher = new HazelcastDispatcher(hazelcast);
		
		AbstractDirectoryCreationStrategy dirCreator = new BarabasiAlbertDirectoryCreationStrategy(dao, dispatcher);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dispatcher, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, id);
		
		Map<String,Double> overallMeasurements = new LinkedHashMap<String,Double>();
		
		if(numberOfDirs > 0){
			log.info("Dir creation started");
			long start = System.currentTimeMillis();
			nsGen.generateDirs(numberOfDirs);
			long end = System.currentTimeMillis();
			log.info("{} dirs generated in: {}", numberOfDirs, (end-start)/1000.0);
			try {
				ProgressBarrier.awaitOperationCompletion(numberOfDirs);
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
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
		}
		
		if(numberOfFiles > 0){
			try {
				log.debug("Going to sleep for {} seconds after directory creation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}

			log.info("File creation started");
			long start = System.currentTimeMillis();
			nsGen.generateFiles(numberOfFiles);
			long end = System.currentTimeMillis();
			log.info("{} files generated in: {}", numberOfFiles, (end-start)/1000.0);
			try {
				ProgressBarrier.awaitOperationCompletion(numberOfFiles);
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
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
		}
		
		collectAndExportMeasurements(dispatcher, "namespace", overallMeasurements);
		
		if(numberOfOperations > 0){
			overallMeasurements = new LinkedHashMap<String,Double>();
			try {
				log.debug("Going to sleep for {} seconds after namespace generation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}

			log.info("Workload generation started");
			long start = System.currentTimeMillis();
			WorkloadGenerator wlGen = new WorkloadGenerator(dispatcher, numberOfOperations, numberOfDirs, numberOfFiles);
			wlGen.generate();
			long end = System.currentTimeMillis();
			log.info("{} operations generated in: {}", numberOfOperations, (end-start)/1000.0);
			try {
				ProgressBarrier.awaitOperationCompletion(numberOfOperations);
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
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
			collectAndExportMeasurements(dispatcher, "workload", overallMeasurements);
		}
	}
	
	private static void collectAndExportMeasurements(IDispatcher dispatcher, String generationStepName, Map<String,Double> overallMeasurements){
		MeasurementDataCollection measurements = collectMeasurements(dispatcher);
		String measurementString = getAndExportMeasurementText(measurements, overallMeasurements);
		exportMeasurementTextToFile(measurementString, generationStepName);
		log.debug("Resetting local and remote measurements");
		dispatcher.dispatch(new MeasurementsReset());
		measurements.reset();
	}

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
	
	private static String getAndExportMeasurementText(MeasurementDataCollection measurements, Map<String,Double> overallMeasurements){
		String measurementString = "";
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextMeasurementsExporter exporter = new TextMeasurementsExporter(baos);
			String OVERALL = "OVERALL";
			Iterator<String> iterator = overallMeasurements.keySet().iterator();
			while(iterator.hasNext()){
				String measurement = iterator.next();
				double value = overallMeasurements.get(measurement);
				exporter.write(OVERALL, measurement, value);
			}
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
