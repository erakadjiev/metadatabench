/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.reporter.Reporter;

import edu.cmu.pdl.metadatabench.common.Config;

/**
 * Collects latency measurements, and reports them when requested.
 * 
 * Changes to YCSB class: added warm-up time, added reporting of exceptions, 
 * added method to get raw measurement data, further smaller refactorings.
 * 
 * @author cooperb
 * @author emil.rakadjiev
 * 
 */
public class Measurements{
	/** @see edu.cmu.pdl.metadatabench.common.Config#isMeasurementHistogram() */
	public static final String MEASUREMENT_TYPE = "measurementtype";
	public static final String MEASUREMENT_TYPE_HISTOGRAM = "histogram";
	public static final String MEASUREMENT_TYPE_TIMESERIES = "timeseries";
	/** @see edu.cmu.pdl.metadatabench.common.Config#getMeasurementWarmUpTime() */
	public static final String MEASUREMENT_WARM_UP = "measurementwarmup";
	private static final String MEASUREMENT_WARM_UP_TIME_DEFAULT = String.valueOf(Config.getMeasurementWarmUpTime());
	public static final String NODE_ID = "nodeid";

	static Measurements singleton = null;
	static Properties measurementproperties = null;
	
	public static void setProperties(Properties props) {
		measurementproperties = props;
	}

	/**
	 * Return the singleton Measurements object.
	 */
	public synchronized static Measurements getMeasurements() {
		if (singleton == null) {
			singleton = new Measurements(measurementproperties);
		}
		return singleton;
	}

	HashMap<String, OneMeasurement> data;
	boolean histogram = true;

	private int warmUpTime;
	private long firstMeasurementTimeStamp;
	private boolean warmUpDone;
	
	private Properties _props;
	
	private Logger log;

	/**
	 * Create a new object with the specified properties.
	 */
	public Measurements(Properties props) {
		data = new HashMap<String, OneMeasurement>();

		_props = props;

		warmUpTime = Integer.parseInt(_props.getProperty(MEASUREMENT_WARM_UP, MEASUREMENT_WARM_UP_TIME_DEFAULT));
		firstMeasurementTimeStamp = 0;
		warmUpDone = (warmUpTime == 0) ? true : false;
		
		if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_HISTOGRAM).compareTo("histogram") == 0) {
			histogram = true;
		} else {
			histogram = false;
		}
		
		this.log = LoggerFactory.getLogger(Measurements.class);
	}

	OneMeasurement constructOneMeasurement(String name) {
		if (histogram) {
			return new OneMeasurementHistogram(name, _props);
		} else {
			return new OneMeasurementTimeSeries(name, _props);
		}
	}

	/**
	 * Resets the measurement data
	 */
	public void cleanMeasurement() {
		data = new HashMap<String, OneMeasurement>();
		firstMeasurementTimeStamp = 0;
		warmUpDone = (warmUpTime == 0) ? true : false;
	}

	/**
	 * Report a single value of a single metric. E.g. for read latency,
	 * operation="READ" and latency is the measured value.
	 * 
	 * Change to original method: added warm-up time, when measurements are ignored.
	 */
	public synchronized void measure(String operation, int latency) {
		if(!warmUpDone){
			long now = System.currentTimeMillis();
			if(firstMeasurementTimeStamp == 0){
				log.info("Will start measurements in {} seconds (warm-up time)", warmUpTime/1000);
				firstMeasurementTimeStamp = now;
			} else if((now - firstMeasurementTimeStamp) > warmUpTime){
				warmUpDone = true;
				log.info("Warm-up done, starting measurements.");
				doMeasurement(operation, latency);
			}
		} else {
			doMeasurement(operation, latency);
		}
	}
	
	private void doMeasurement(String operation, int latency){
		initOperation(operation);
		try {
			data.get(operation).measure(latency);
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			log.debug("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing", e);
		}
	}
	
	/**
	 * Report a return code for a single DB operaiton.
	 */
	public void reportReturnCode(String operation, int code) {
		initOperation(operation);
		data.get(operation).reportReturnCode(code);
	}
	
	/**
	 * Reports a failed operation
	 * 
	 * @param operation The name of the operation that has failed
	 * @param exceptionType The name of the exception that has occured
	 */
	public void reportException(String operation, String exceptionType) {
		initOperation(operation);
		data.get(operation).reportException(exceptionType);
	}

	/**
	 * Export the current measurements to a suitable format.
	 * 
	 * @param exporter
	 *            Exporter representing the type of format to write to.
	 * @throws IOException
	 *             Thrown if the export failed.
	 */
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		for (OneMeasurement measurement : data.values()) {
			measurement.exportMeasurements(exporter);
		}
	}

	/**
	 * Return a one line summary of the measurements.
	 */
	public String getSummary(Reporter rep) {
		String ret = "";
		DecimalFormat d = new DecimalFormat("#.##");
		for (OneMeasurement m : data.values()) {
			double latency = m.getAvgLatency();
			if (rep != null) {
				rep.send("latency_" + m.getName().toLowerCase(), latency);
			}
			ret += "[" + m.getName() + " AverageLatency(ms)=" + d.format(latency) + "]";
		}
		return ret;
	}
	
	/**
	 * Gets the raw measurements data
	 * @return The raw measurement data
	 */
	public MeasurementData getMeasurementData(){
		return new MeasurementData(data, histogram);
	}
	
	/**
	 * Gets the raw measurements data including the id of this node
	 * @return The raw measurement data including the id of this node
	 */
	public MeasurementDataForNode getMeasurementDataForNode(){
		int nodeId = 0;
		String prop = _props.getProperty(NODE_ID);
		if(prop != null){
			nodeId = Integer.parseInt(prop);
		}
		return new MeasurementDataForNode(nodeId, data, histogram);
	}
	
	private void initOperation(String operation){
		if (!data.containsKey(operation)) {
			synchronized (this) {
				if (!data.containsKey(operation)) {
					data.put(operation, constructOneMeasurement(operation));
				}
			}
		}
	}
}
