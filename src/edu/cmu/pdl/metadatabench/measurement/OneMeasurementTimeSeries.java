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
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import edu.cmu.pdl.metadatabench.common.Config;

@SuppressWarnings("serial")
class SeriesUnit implements Serializable, Cloneable {
	/**
	 * @param time
	 * @param average
	 */
	public SeriesUnit(long time, double average) {
		this.time = time;
		this.average = average;
	}

	public long time;
	public double average;
	
	public SeriesUnit clone() throws CloneNotSupportedException {
		return (SeriesUnit) super.clone();
	}
}

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
@SuppressWarnings("serial")
public class OneMeasurementTimeSeries extends OneMeasurement {
	/**
	 * Granularity for time series; measurements will be averaged in chunks of
	 * this granularity. Units are milliseconds.
	 */
	public static final String GRANULARITY = "timeseries.granularity";
	public static final String GRANULARITY_DEFAULT = String.valueOf(Config.getMeasurementTimeSeriesGranularity());

	int _granularity;
	Vector<SeriesUnit> _measurements;

	long start = -1;
	long currentunit = -1;
	int count = 0;
	int sum = 0;
	int operations = 0;
	long totallatency = 0;

	// keep a windowed version of these stats for printing status
	int windowoperations = 0;
	long windowtotallatency = 0;

	int min = -1;
	int max = -1;

	private HashMap<Integer, int[]> returncodes;

	private Logger log;
	
	public OneMeasurementTimeSeries(String name, Properties props) {
		super(name);
		_granularity = Integer.parseInt(props.getProperty(GRANULARITY, GRANULARITY_DEFAULT));
		_measurements = new Vector<SeriesUnit>();
		returncodes = new HashMap<Integer, int[]>();
		log = LoggerFactory.getLogger(OneMeasurementTimeSeries.class);
	}

	void checkEndOfUnit(boolean forceend) {
		long now = System.currentTimeMillis();

		if (start < 0) {
			currentunit = 0;
			start = now;
		}

		long unit = ((now - start) / _granularity) * _granularity;

		if ((unit > currentunit) || (forceend)) {
			double avg = ((double) sum) / ((double) count);
			addUnit(currentunit, avg);

			currentunit = unit;

			count = 0;
			sum = 0;
		}
	}

	private void addUnit(long unit, double avg) {
		long unitNumber = unit / _granularity;
		int size = _measurements.size();
		if(unitNumber < size){
			SeriesUnit su = _measurements.get((int)unitNumber-1);
			if(unit != su.time){
				log.error("Error during time series measurement. Time step mismatch with existing measurement unit)");
				log.debug("At position {} {} does not equal {}", unitNumber, unit, su.time);
			}
			su.average = (su.average + avg) / 2;
		} else {
			_measurements.add(new SeriesUnit(unit, avg));
		}
		
	}

	@Override
	public void measure(int latency) {
		checkEndOfUnit(false);

		count++;
		sum += latency;
		totallatency += latency;
		operations++;
		windowoperations++;
		windowtotallatency += latency;

		if (latency > max) {
			max = latency;
		}

		if ((latency < min) || (min < 0)) {
			min = latency;
		}
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		checkEndOfUnit(true);

		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency(ms)", (((double) totallatency) / ((double) operations)));
		exporter.write(getName(), "MinLatency(ms)", min);
		exporter.write(getName(), "MaxLatency(ms)", max);

		// TODO: 95th and 99th percentile latency

		for (Integer I : returncodes.keySet()) {
			int[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}

		Set<String> exceptionSet = exceptions.keySet();
		for (String exception : exceptionSet){
			exporter.write(getName(), exception, exceptions.get(exception));
		}
		
		for (SeriesUnit unit : _measurements) {
			exporter.write(getName(), Long.toString(unit.time), unit.average);
		}
	}

	@Override
	public void reportReturnCode(int code) {
		Integer Icode = code;
		if (!returncodes.containsKey(Icode)) {
			int[] val = new int[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;

	}

	@Override
	public double getAvgLatency() {
		if (windowoperations == 0) {
			return 0;
		}
		double report = ((double) windowtotallatency) / ((double) windowoperations);
		windowtotallatency = 0;
		windowoperations = 0;
		return report;
	}

	@Override
	public String getSummary() {
		if (windowoperations == 0) {
			return "";
		}
		DecimalFormat d = new DecimalFormat("#.##");
		double report = ((double) windowtotallatency) / ((double) windowoperations);
		windowtotallatency = 0;
		windowoperations = 0;
		return "[" + getName() + " AverageLatency(ms)=" + d.format(report) + "]";
	}

	@Override
	public void addMeasurement(OneMeasurement measurement) {
		OneMeasurementTimeSeries measurementTS = (OneMeasurementTimeSeries) measurement;
		if(_granularity != measurementTS._granularity){
			log.error("Error: Measurement cannot be added, because the two time series have a different granularity.");
		} else{
			combineTimeSeriesVectors(_measurements, measurementTS._measurements);
			count += measurementTS.count;
			if(currentunit == measurementTS.currentunit){
				sum += measurementTS.sum;
				totallatency += measurementTS.totallatency;
			}
			operations += measurementTS.operations;
			windowoperations += measurementTS.windowoperations;
			windowtotallatency += measurementTS.windowtotallatency;
			if(min > measurementTS.min){
				min = measurementTS.min;
			}
			if(max < measurementTS.max){
				max = measurementTS.max;
			}
			combineReturnCodeMaps(returncodes, measurementTS.returncodes);
			combineExceptionMaps(exceptions, measurementTS.exceptions);
		}
		
	}

	private void combineTimeSeriesVectors(Vector<SeriesUnit> vector, Vector<SeriesUnit> vectorNew) {
		int size = vector.size();
		int sizeNew = vectorNew.size();
		SeriesUnit unit = null;
		SeriesUnit unitNew = null;
		for(int i = 0; i < sizeNew; i++){
			unitNew = vectorNew.get(i);
			if(i < size){
				unit = vector.get(i);
				unit.average = (unit.average + unitNew.average) / 2;
			} else {
				try {
					vector.add(unitNew.clone());
				} catch (CloneNotSupportedException e) {
					log.warn("Time series vector cannot be cloned, storing original object. Some measurement data may get overwritten.");
					vector.add(unitNew);
				}
			}
		}
	}
	
	public OneMeasurementTimeSeries clone() throws CloneNotSupportedException {
		OneMeasurementTimeSeries clone = (OneMeasurementTimeSeries) super.clone();
		clone._measurements = new Vector<SeriesUnit>();
		int size = _measurements.size();
		for(int i = 0 ; i < size ; i++){
			SeriesUnit unit = _measurements.get(i);
			clone._measurements.add(i, unit.clone());
		}
		clone.returncodes = cloneReturnCodeMap(returncodes);
		clone.exceptions = cloneExceptionMap(exceptions);
		return clone;
	}

}
