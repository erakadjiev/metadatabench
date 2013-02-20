package edu.cmu.pdl.metadatabench.cluster;

import java.io.Serializable;
import java.util.concurrent.Callable;

import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;
import edu.cmu.pdl.metadatabench.measurement.Measurements;

@SuppressWarnings("serial")
public class MeasurementsCollect implements Callable<MeasurementDataForNode>, Serializable {

	@Override
	public MeasurementDataForNode call() throws Exception {
		return Measurements.getMeasurements().getMeasurementDataForNode();
	}

}
