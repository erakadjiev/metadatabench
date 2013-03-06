package edu.cmu.pdl.metadatabench.slave.progress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReport;

public class ProgressReporter implements Runnable {

	private static int id;
	private static IDispatcher dispatcher;
	private static long reportFrequencyMillis;
	private static long lastReportedNumber;
	
	private static volatile boolean stopFlag = false;
	
	private Logger log;
	
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
	
	public static void reset(){
		lastReportedNumber = 0;
	}
	
	public static void stop(){
		stopFlag = true;
	}

}
