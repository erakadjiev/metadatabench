package edu.cmu.pdl.metadatabench.cluster.communication;

import java.util.Collection;

import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsCollect;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressFinished;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReport;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

public interface IDispatcher {

	public void dispatch(SimpleOperation operation);
	public void dispatch(ProgressReport report);
	public void dispatch(ProgressReset reset);
	public void dispatch(ProgressFinished finish);
	public void dispatch(MeasurementsReset reset);
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception;
	
}
