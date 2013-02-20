package edu.cmu.pdl.metadatabench.cluster;

import java.util.Collection;

import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

public interface IOperationDispatcher {

	public void dispatch(SimpleOperation operation);
	public void dispatch(ProgressReport report);
	public void dispatch(ProgressReset reset);
	public void dispatch(ProgressFinished finish);
	public void dispatch(MeasurementsReset reset);
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception;
	
}
