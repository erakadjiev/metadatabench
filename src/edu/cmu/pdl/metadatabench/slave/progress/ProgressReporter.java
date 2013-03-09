package edu.cmu.pdl.metadatabench.slave.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReport;

/**
 * Periodically reports the progress of this node, that is, the number of executed operations, to the master.
 * @author emil.rakadjiev
 *
 */
public class ProgressReporter implements Runnable {

	private static int id;
	private static IDispatcher dispatcher;
	private static long reportFrequencyMillis;
	private static long lastReportedNumber;
	
	private static volatile boolean stopFlag = false;
	
	private Logger log;
	
	/**
	 * @param nodeId The id of this node
	 * @param dispatcher The dispatcher to use for sending messages to the master
	 * @param reportFrequencyMillis The frequency with which to report the progress to the master
	 */
	public ProgressReporter(int nodeId, IDispatcher dispatcher, long reportFrequencyMillis) {
		ProgressReporter.id = nodeId;
		ProgressReporter.dispatcher = dispatcher;
		ProgressReporter.reportFrequencyMillis = reportFrequencyMillis;
		ProgressReporter.lastReportedNumber = 0;
		this.log = LoggerFactory.getLogger(ProgressReporter.class);
	}

	@Override
	public void run() {
		while(!stopFlag){
			long ops = Progress.getOperationsDone();
			// if there was progress done since the last report, send a new report
			if(ops > lastReportedNumber){
				log.info("{} operations done", ops);
				lastReportedNumber = ops;
				dispatcher.dispatch(new ProgressReport(id, ops));
			}
			try {
				Thread.sleep(reportFrequencyMillis);
			} catch (InterruptedException e) {
				log.warn("Thread interrupted while sleeping", e);
			}
		}
	}
	
	/**
	 * Resets the status of the progress reporter
	 */
	public static void reset(){
		lastReportedNumber = 0;
	}
	
	/**
	 * Stops the progress reporter
	 */
	public static void stop(){
		stopFlag = true;
	}

}
