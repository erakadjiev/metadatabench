package edu.cmu.pdl.metadatabench.measurement;

import java.util.HashMap;

/**
 * {@link edu.cmu.pdl.metadatabench.measurement.MeasurementData} including the id of the node where the measurements 
 * were recorded.
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class MeasurementDataForNode extends MeasurementData{

	private int nodeId;
	
	/**
	 * @param nodeId The id of the node where the measurements were recorded.
	 * @param data The raw measurement data
	 * @param histogram True if the measurement type is histogram, false if it's time series
	 */
	public MeasurementDataForNode(int nodeId, HashMap<String, OneMeasurement> data,	boolean histogram) {
		super(data, histogram);
		this.nodeId = nodeId;
	}
	
	/**
	 * Gets the id of the node where the measurements were recorded
	 * @return The id of the node where the measurements were recorded
	 */
	public int getNodeId(){
		return nodeId;
	}

}
