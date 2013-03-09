package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;
import java.util.concurrent.Callable;

import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;
import edu.cmu.pdl.metadatabench.measurement.Measurements;

/**
 * A task that collects the measurements from a slave and returns it to the master
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class MeasurementsCollect implements Callable<MeasurementDataForNode>, Serializable {

	@Override
	public MeasurementDataForNode call() throws Exception {
		return Measurements.getMeasurements().getMeasurementDataForNode();
	}

}
