package edu.cmu.pdl.metadatabench.cluster.communication.messages;

import java.io.Serializable;

import edu.cmu.pdl.metadatabench.measurement.Measurements;

@SuppressWarnings("serial")
public class MeasurementsReset implements Runnable, Serializable {

	@Override
	public void run() {
		Measurements.getMeasurements().cleanMeasurement();
	}

}
