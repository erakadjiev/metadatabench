package edu.cmu.pdl.metadatabench.slave;

import edu.cmu.pdl.metadatabench.cluster.IOperationDispatcher;
import edu.cmu.pdl.metadatabench.cluster.ProgressReport;

public class ProgressReporter implements Runnable {

	private int id;
	private IOperationDispatcher dispatcher;
	private long reportFrequencyMillis;
	private int lastReportedNumber;
	
	private static volatile boolean stopFlag = false;
	
	public ProgressReporter(int nodeId, IOperationDispatcher dispatcher, long reportFrequencyMillis) {
		this.id = nodeId;
		this.dispatcher = dispatcher;
		this.reportFrequencyMillis = reportFrequencyMillis;
		this.lastReportedNumber = 0;
	}

	@Override
	public void run() {
		while(!stopFlag){
			int ops = Progress.getOperationsDone();
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
	
	public static void stop(){
		stopFlag = true;
	}

}
