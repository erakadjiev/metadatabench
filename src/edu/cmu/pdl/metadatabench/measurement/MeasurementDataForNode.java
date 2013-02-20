package edu.cmu.pdl.metadatabench.measurement;

import java.util.HashMap;

@SuppressWarnings("serial")
public class MeasurementDataForNode extends MeasurementData{

	private int nodeId;
	
	public MeasurementDataForNode(int nodeId, HashMap<String, OneMeasurement> data,	boolean histogram) {
		super(data, histogram);
		this.nodeId = nodeId;
	}
	
	public int getNodeId(){
		return nodeId;
	}

}
