package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

@SuppressWarnings("serial")
public class MeasurementData implements Serializable, Cloneable {

	private HashMap<String, OneMeasurement> data;
	private boolean histogram;
	
	public MeasurementData(HashMap<String, OneMeasurement> data, boolean histogram) {
		this.data = data;
		this.histogram = histogram;
	}

	public HashMap<String, OneMeasurement> getData() {
		return data;
	}

	public boolean isHistogram() {
		return histogram;
	}
	
	public void addMeasurementData(MeasurementData measurementData){
		if(measurementData.isHistogram() != histogram){
			System.out.println("Error: Cannot add incompatible measurement types (histogram and time series).");
		} else {
			HashMap<String, OneMeasurement> dataToAdd = measurementData.getData();
			Set<String> operations = dataToAdd.keySet();
			for(String operation : operations){
				OneMeasurement measurement = dataToAdd.get(operation);
				if (!data.containsKey(operation)) {
					synchronized (this) {
						if (!data.containsKey(operation)) {
							try {
								data.put(operation, measurement.clone());
							} catch (CloneNotSupportedException e) {
								e.printStackTrace();
								data.put(operation, measurement);
							}
						}
					}
				} else {
					data.get(operation).addMeasurement(measurement);
				}
			}
		}
	}
	
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		for (OneMeasurement measurement : data.values()) {
			measurement.exportMeasurements(exporter);
		}
	}
	
	public MeasurementData clone() throws CloneNotSupportedException {
		MeasurementData clone = (MeasurementData) super.clone();
		HashMap<String, OneMeasurement> dataNew = new HashMap<String, OneMeasurement>();
		Set<String> keySet = data.keySet();
		for(String key : keySet){
			OneMeasurement measurement = data.get(key);
			dataNew.put(key, measurement.clone());
		}
		clone.data = dataNew;
		return clone;
	}
	
}
