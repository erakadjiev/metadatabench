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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import edu.cmu.pdl.metadatabench.common.Config;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ
 * LATENCY.
 * 
 * Changes to original YCSB class: added support recording of exceptions, added support for adding 
 * measurement data from another measurement, added cloning of measurement object. 
 * 
 * @author cooperb
 * @author emil.rakadjiev
 * 
 */
@SuppressWarnings("serial")
public class OneMeasurementHistogram extends OneMeasurement {
	public static final String BUCKETS = "histogram.buckets";
	public static final String BUCKETS_DEFAULT = String.valueOf(Config.getMeasurementHistogramBuckets());

	int _buckets;
	int[] histogram;
	int histogramoverflow;
	int operations;
	long totallatency;

	// keep a windowed version of these stats for printing status
	int windowoperations;
	long windowtotallatency;

	int min;
	int max;
	HashMap<Integer, int[]> returncodes;
	
	private Logger log;

	public OneMeasurementHistogram(String name, Properties props) {
		super(name);
		_buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
		histogram = new int[_buckets];
		histogramoverflow = 0;
		operations = 0;
		totallatency = 0;
		windowoperations = 0;
		windowtotallatency = 0;
		min = -1;
		max = -1;
		returncodes = new HashMap<Integer, int[]>();
		log = LoggerFactory.getLogger(OneMeasurementHistogram.class);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code) {
		Integer Icode = code;
		if (!returncodes.containsKey(Icode)) {
			int[] val = new int[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public synchronized void measure(int latency) {
		if (latency >= _buckets) {
			histogramoverflow++;
		} else {
			histogram[latency]++;
		}
		operations++;
		totallatency += latency;
		windowoperations++;
		windowtotallatency += latency;

		if ((min < 0) || (latency < min)) {
			min = latency;
		}

		if ((max < 0) || (latency > max)) {
			max = latency;
		}
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency(ms)",	(((double) totallatency) / ((double) operations)));
		exporter.write(getName(), "MinLatency(ms)", min);
		exporter.write(getName(), "MaxLatency(ms)", max);

		int opcounter = 0;
		boolean done95th = false;
		for (int i = 0; i < _buckets; i++) {
			opcounter += histogram[i];
			if ((!done95th)	&& (((double) opcounter) / ((double) operations) >= 0.95)) {
				exporter.write(getName(), "95thPercentileLatency(ms)", i);
				done95th = true;
			}
			if (((double) opcounter) / ((double) operations) >= 0.99) {
				exporter.write(getName(), "99thPercentileLatency(ms)", i);
				break;
			}
		}

		for (Integer I : returncodes.keySet()) {
			int[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}

		Set<String> exceptionSet = exceptions.keySet();
		for (String exception : exceptionSet){
			exporter.write(getName(), exception, exceptions.get(exception));
		}
		
		for (int i = 0; i < _buckets; i++) {
			exporter.write(getName(), Integer.toString(i), histogram[i]);
		}
		exporter.write(getName(), ">" + _buckets, histogramoverflow);
	}

	@Override
	public double getAvgLatency() {
		if (windowoperations == 0) {
			return 0;
		}
//		DecimalFormat d = new DecimalFormat("#.##");
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addMeasurement(OneMeasurement measurement) {
		OneMeasurementHistogram measurementHistogram = (OneMeasurementHistogram) measurement;
		if(_buckets != measurementHistogram._buckets){
			log.error("Error: Measurement cannot be added, because the two histograms have a different number of buckets.");
		} else{
			combineHistogramArrays(histogram, measurementHistogram.histogram);
			histogramoverflow += measurementHistogram.histogramoverflow;
			operations += measurementHistogram.operations;
			totallatency += measurementHistogram.totallatency;
			windowoperations += measurementHistogram.windowoperations;
			windowtotallatency += measurementHistogram.windowtotallatency;
			if(min > measurementHistogram.min){
				min = measurementHistogram.min;
			}
			if(max < measurementHistogram.max){
				max = measurementHistogram.max;
			}
			combineReturnCodeMaps(returncodes, measurementHistogram.returncodes);
			combineExceptionMaps(exceptions, measurementHistogram.exceptions);
		}
		
	}

	/**
	 * Merges two histogram arrays
	 * 
	 * @param histogram The original histogram array that will be kept
	 * @param histogramNew The new histogram array whose data will be merged into the original array
	 */
	private void combineHistogramArrays(int[] histogram, int[] histogramNew){
		int length = histogram.length;
		if(length != histogramNew.length){
			log.error("Error: Cannot combine the two histogram arrays, because they have a different size.");
		} else {
			for(int i = 0; i < length ; i++){
				histogram[i] += histogramNew[i];
			}
		}
	}
	
	/**
	 * Deep-clones this measurement
	 * @return the cloned measurement
	 */
	public OneMeasurementHistogram clone() throws CloneNotSupportedException {
		OneMeasurementHistogram clone = (OneMeasurementHistogram) super.clone();
		clone.histogram = histogram.clone();
		clone.returncodes = cloneReturnCodeMap(returncodes);
		clone.exceptions = cloneExceptionMap(exceptions);
		return clone;
	}

}
