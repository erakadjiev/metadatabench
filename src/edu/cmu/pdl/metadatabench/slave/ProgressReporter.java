package edu.cmu.pdl.metadatabench.slave;

import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.ProgressReport;

public class ProgressReporter implements Runnable {

	private static int id;
	private static IOperationDispatcher dispatcher;
	private static long reportFrequencyMillis;
	private static long lastReportedNumber;
	
	private static volatile boolean stopFlag = false;
	
	public ProgressReporter(int nodeId, IOperationDispatcher dispatcher, long reportFrequencyMillis) {
		ProgressReporter.id = nodeId;
		ProgressReporter.dispatcher = dispatcher;
		ProgressReporter.reportFrequencyMillis = reportFrequencyMillis;
		ProgressReporter.lastReportedNumber = 0;
	}

	@Override
	public void run() {
		while(!stopFlag){
			long ops = Progress.getOperationsDone();
			if(ops > lastReportedNumber){
				System.out.println(ops + " operations done");
				lastReportedNumber = ops;
				dispatcher.dispatch(new ProgressReport(id, ops));
			}
			try {
				Thread.sleep(reportFrequencyMillis);
			} catch (InterruptedException e) {
				e.printStackTrace();
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
