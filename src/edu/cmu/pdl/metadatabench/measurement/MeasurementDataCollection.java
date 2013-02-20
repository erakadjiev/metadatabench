package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MeasurementDataCollection {
	
	public static MeasurementDataCollection instance;
	
	private static MeasurementData dataAggregate;
	private static Map<Integer,MeasurementData> dataPerNode = new HashMap<Integer,MeasurementData>();
	
	public static MeasurementDataCollection getInstance(){
		if(instance == null){
			synchronized(MeasurementDataCollection.class){
				if(instance == null){
					instance = new MeasurementDataCollection();
				}
			}
		}
		
		return instance;
	}
	
	private MeasurementDataCollection() {}
	
	public void reset(){
		dataAggregate = null;
		dataPerNode = new HashMap<Integer,MeasurementData>();
	}
	
	public synchronized void addMeasurementData(MeasurementData measurementData){
		if(measurementData instanceof MeasurementDataForNode){
			int nodeId = ((MeasurementDataForNode)measurementData).getNodeId();
			if(!dataPerNode.containsKey(nodeId)){
				dataPerNode.put(nodeId, measurementData);
			} else {
				dataPerNode.get(nodeId).addMeasurementData(measurementData);
			}
		}
		if(dataAggregate == null){
			try {
				dataAggregate = measurementData.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				dataAggregate = measurementData;
			}
		} else {
			dataAggregate.addMeasurementData(measurementData);
		}
	}
	
	public Map<Integer,MeasurementData> getNodeMeasurementData(){
		return dataPerNode;
	}
	
	public void exportMeasurements(MeasurementsExporterExt exporter) throws IOException {
		exporter.write("Exporting measurements collected from " + dataPerNode.size() + " nodes");
		dataAggregate.exportMeasurements(exporter);
	}
	
	public void exportMeasurementsPerNode(MeasurementsExporterExt exporter) throws IOException {
		Set<Integer> nodeIds = dataPerNode.keySet();
		for(int nodeId : nodeIds){
			exporter.write("Measurements for node " + nodeId);
			dataPerNode.get(nodeId).exportMeasurements(exporter);
		}
	}

}
