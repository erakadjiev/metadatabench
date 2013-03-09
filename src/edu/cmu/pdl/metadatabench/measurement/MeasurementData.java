package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Raw data of a {@link edu.cmu.pdl.metadatabench.measurement.Measurements}. Provides functionality to add 
 * further measurement data, to export the measurement data and to clone the measurement data.
 * 
 * @author emil.rakadjiev
 *
 */
@SuppressWarnings("serial")
public class MeasurementData implements Serializable, Cloneable {

	private HashMap<String, OneMeasurement> data;
	private boolean histogram;
	private Logger log;
	
	/**
	 * @param data The raw measurement data
	 * @param histogram True if the measurement type is histogram, false if it's time series
	 */
	public MeasurementData(HashMap<String, OneMeasurement> data, boolean histogram) {
		this.data = data;
		this.histogram = histogram;
		this.log = LoggerFactory.getLogger(MeasurementData.class);
	}

	/**
	 * Gets the raw measurement data
	 * 
	 * @return The raw measurement data
	 */
	public HashMap<String, OneMeasurement> getData() {
		return data;
	}

	/**
	 * Returns true if the measurement type is histogram, false if it's time series
	 * @return True if the measurement type is histogram, false if it's time series
	 */
	public boolean isHistogram() {
		return histogram;
	}
	
	/**
	 * Add data from another measurement (combines the data in this measurement object). The measurement 
	 * object that is added is cloned, so that the changes do not affect the object from which the data was copied.
	 * @param measurementData The measurement data to merge into this measurement 
	 */
	public void addMeasurementData(MeasurementData measurementData){
		if(measurementData.isHistogram() != histogram){
			log.error("Error: Cannot add incompatible measurement types (histogram and time series).");
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
								log.warn("Measurement object cannot be cloned, storing original object. Some measurement data may get overwritten.");
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
	
	/**
	 * Exports the measurement data, for example to text format
	 * @param exporter The exporter to use
	 * @throws IOException
	 */
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		for (OneMeasurement measurement : data.values()) {
			measurement.exportMeasurements(exporter);
		}
	}
	
	/**
	 * Deep-clones this measurement data object
	 * @return the cloned measurement data object
	 */
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
