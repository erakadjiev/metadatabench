package edu.cmu.pdl.metadatabench.cluster;

public interface IOperationDispatcher {

	public void dispatch(SimpleOperation operation);
	public void dispatch(ProgressReport report);
	public void dispatch(ProgressReset reset);
	public void dispatch(ProgressFinished finish);
	
}
