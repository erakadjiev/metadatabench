package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.measurement.Measurements;

/**
 * A message notifying a slave that a generation step (directory creation, file creation or workload) 
 * has been completed and it should reset its measurements.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class MeasurementsReset implements Runnable, Serializable {

	@Override
	public void run() {
		Measurements.getMeasurements().cleanMeasurement();
	}

}
