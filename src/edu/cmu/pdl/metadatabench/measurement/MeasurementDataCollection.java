package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Stores the measurement data objects for all of the nodes separately and aggregated as well.
 * 
 * @author emil.rakadjiev
 *
 */
public class MeasurementDataCollection {
	
	/** Singleton */
	public static MeasurementDataCollection instance;
	
	/** Aggregated measurements from all nodes */
	private static MeasurementData dataAggregate;
	/** Measurements for each node separately */
	private static Map<Integer,MeasurementData> dataPerNode = new HashMap<Integer,MeasurementData>();
	
	/**
	 * Gets the singleton instance
	 * @return The singleton instance
	 */
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
	
	/**
	 * Resets the measurement data
	 */
	public void reset(){
		dataAggregate = null;
		dataPerNode = new HashMap<Integer,MeasurementData>();
	}
	
	/**
	 * Add new measurement data (both to the per-node and the aggregated measurements). The measurement 
	 * object that is added to the aggregated measurements is cloned, so that the changes do not affect 
	 * the object from which the data was copied.
	 * @param measurementData The measurement data to add 
	 */
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
	
	/**
	 * Gets the measurement data for all nodes separately
	 * @return The measurement data for all nodes separately
	 */
	public Map<Integer,MeasurementData> getNodeMeasurementData(){
		return dataPerNode;
	}
	
	/**
	 * Exports the aggregated measurement data, for example to text format
	 * @param exporter The exporter to use
	 * @throws IOException
	 */
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		exporter.write("OVERALL", "Number of slaves", dataPerNode.size());
		dataAggregate.exportMeasurements(exporter);
	}
	
	/**
	 * Exports the measurement data for each node separately, for example to text format
	 * @param exporter The exporter to use
	 * @throws IOException
	 */
	public void exportMeasurementsPerNode(MeasurementsExporter exporter) throws IOException {
		Set<Integer> nodeIds = dataPerNode.keySet();
		for(int nodeId : nodeIds){
			exporter.write("OVERALL", "Node number", nodeId);
			dataPerNode.get(nodeId).exportMeasurements(exporter);
		}
	}

}
