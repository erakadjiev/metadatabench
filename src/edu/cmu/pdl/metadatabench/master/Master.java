package edu.cmu.pdl.metadatabench.master;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;

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
import edu.cmu.pdl.metadatabench.measurement.TextMeasurementsExporterExt;

public class Master {

	public static final int SLEEP_AFTER_GENERATION_STEP_MILLIS = 2500;
	
	public static void start(HazelcastInstance hazelcast, int id, int masters, int numberOfDirs, int numberOfFiles, int numberOfOperations){
		Logger log = LoggerFactory.getLogger(Master.class);
		
		INamespaceMapDAO dao = new HazelcastMapDAO(hazelcast);
		IDispatcher dispatcher = new HazelcastDispatcher(hazelcast);
		
		AbstractDirectoryCreationStrategy dirCreator = new BarabasiAlbertDirectoryCreationStrategy(dao, dispatcher, "/workDir", masters);
		AbstractFileCreationStrategy fileCreator = new ZipfianFileCreationStrategy(dispatcher, numberOfDirs);
		NamespaceGenerator nsGen = new NamespaceGenerator(dirCreator, fileCreator, id, masters);
		
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
			log.info("Dir creation done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", numberOfDirs/creationTime);
			log.debug("Resetting local and remote progress");
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());

			try {
				log.debug("Going to sleep for {} seconds after directory creation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}
		}
		
		
		if(numberOfFiles > 0){
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
			log.info("File creation done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", numberOfFiles/creationTime);
			log.debug("Resetting local and remote progress");
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
			collectAndExportMeasurements(dispatcher, log);
			
			try {
				log.debug("Going to sleep for {} seconds after file creation", SLEEP_AFTER_GENERATION_STEP_MILLIS);
				Thread.sleep(SLEEP_AFTER_GENERATION_STEP_MILLIS);
			} catch (InterruptedException e) {
				log.warn("Thread was interrupted while sleeping", e);
			}
		}

		
		if(numberOfOperations > 0){
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
			log.info("Workload done in: {} s", creationTime);
			log.info("Throughput: {} ops/s", numberOfOperations/creationTime);
			log.debug("Resetting local and remote progress");
			ProgressBarrier.reset();
			dispatcher.dispatch(new ProgressReset());
			collectAndExportMeasurements(dispatcher, log);
		}
	}
	
	private static void collectAndExportMeasurements(IDispatcher dispatcher, Logger log){
		MeasurementDataCollection measurements = MeasurementDataCollection.getInstance();
		try {
			Collection<MeasurementDataForNode> measurementDataCollection = dispatcher.dispatch(new MeasurementsCollect());
			for(MeasurementDataForNode measurementData : measurementDataCollection){
				measurements.addMeasurementData(measurementData);
			}
		} catch (Exception e) {
			log.error("Exception while collecting measurements from nodes", e);
		}
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextMeasurementsExporterExt exporter = new TextMeasurementsExporterExt(baos);
			measurements.exportMeasurements(exporter);
//			measurements.exportMeasurementsPerNode(exporter);
			exporter.close();
			log.info(baos.toString());
		} catch (IOException e) {
			log.warn("Exception while exporting measurements to a text format", e);
		}
		log.debug("Resetting local and remote measurements");
		dispatcher.dispatch(new MeasurementsReset());
		measurements.reset();
	}

}
