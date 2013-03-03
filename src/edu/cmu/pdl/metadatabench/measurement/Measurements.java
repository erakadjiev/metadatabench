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

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.reporter.Reporter;

/**
 * Collects latency measurements, and reports them when requested.
 * 
 * @author cooperb
 * 
 */
public class Measurements{
	public static final String MEASUREMENT_TYPE = "measurementtype";
	private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";
	public static final String MEASUREMENT_WARM_UP = "measurementwarmup";
	public static final String MEASUREMENT_WARM_UP_DEFAULT = "0";

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

	/**
	 * Create a new object with the specified properties.
	 */
	public Measurements(Properties props) {
		data = new HashMap<String, OneMeasurement>();

		_props = props;

		warmUpTime = Integer.parseInt(_props.getProperty(MEASUREMENT_WARM_UP, MEASUREMENT_WARM_UP_DEFAULT));
		firstMeasurementTimeStamp = 0;
		warmUpDone = (warmUpTime == 0) ? true : false;
		
		if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT).compareTo("histogram") == 0) {
			histogram = true;
		} else {
			histogram = false;
		}
	}

	OneMeasurement constructOneMeasurement(String name) {
		if (histogram) {
			return new OneMeasurementHistogram(name, _props);
		} else {
			return new OneMeasurementTimeSeries(name, _props);
		}
	}

	public void cleanMeasurement() {
		data = new HashMap<String, OneMeasurement>();
		firstMeasurementTimeStamp = 0;
		warmUpDone = (warmUpTime == 0) ? true : false;
	}

	/**
	 * Report a single value of a single metric. E.g. for read latency,
	 * operation="READ" and latency is the measured value.
	 */
	public synchronized void measure(String operation, int latency) {
		if(!warmUpDone){
			long now = System.currentTimeMillis();
			if(firstMeasurementTimeStamp == 0){
				System.out.println("Will start measurements in " + warmUpTime/1000 + " seconds (warm-up time)");
				firstMeasurementTimeStamp = now;
			} else if((now - firstMeasurementTimeStamp) > warmUpTime){
				warmUpDone = true;
				System.out.println("Warm-up done, starting measurements.");
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
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}
	
	/**
	 * Report a return code for a single DB operaiton.
	 */
	public void reportReturnCode(String operation, int code) {
		initOperation(operation);
		data.get(operation).reportReturnCode(code);
	}
	
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
	
	public MeasurementData getMeasurementData(){
		return new MeasurementData(data, histogram);
	}
	
	public MeasurementDataForNode getMeasurementDataForNode(){
		int nodeId = 0;
		String prop = _props.getProperty("nodeId");
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
