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
import java.util.HashMap;
import java.util.Set;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * A single measured metric (such as READ LATENCY)
 * 
 * Changes to original YCSB class: added support recording of exceptions, added support for adding 
 * measurement data from another measurement, added cloning of underlying data structures. 
 */
@SuppressWarnings("serial")
public abstract class OneMeasurement implements Serializable, Cloneable {
	
	String _name;
	
	protected HashMap<String, Integer> exceptions;
	
	public String getName() {
		return _name;
	}

	/**
	 * @param _name
	 */
	public OneMeasurement(String _name) {
		this._name = _name;
		exceptions = new HashMap<String, Integer>();
	}

	public abstract void reportReturnCode(int code);

	/**
	 * Reports a failed operation
	 * 
	 * @param exceptionType The name of the exception that has occured
	 */
	public synchronized void reportException(String exceptionType) {
		Integer count = exceptions.get(exceptionType);
		if(count == null){
			count = 0;
		}
		exceptions.put(exceptionType, ++count);
	}
	
	public abstract void measure(int latency);

	public abstract String getSummary();

	public abstract double getAvgLatency();

	/**
	 * Export the current measurements to a suitable format.
	 * 
	 * @param exporter
	 *            Exporter representing the type of format to write to.
	 * @throws IOException
	 *             Thrown if the export failed.
	 */
	public abstract void exportMeasurements(MeasurementsExporter exporter) throws IOException;
	
	/**
	 * Add data from another measurement (combines the data in this measurement object)
	 * @param measurement The measurement to merge into this measurement 
	 */
	public abstract void addMeasurement(OneMeasurement measurement);
	
	/**
	 * Clones this measurement
	 * @return the cloned measurement
	 */
	public OneMeasurement clone() throws CloneNotSupportedException {
		return (OneMeasurement) super.clone();
	}
	
	/**
	 * Merges two return code maps
	 * 
	 * @param returncodes The original return codes map that will be kept
	 * @param returncodesNew The new return codes map whose data will be merged into the original map
	 */
	protected void combineReturnCodeMaps(HashMap<Integer, int[]> returncodes, HashMap<Integer, int[]> returncodesNew) {
		Set<Integer> codes = returncodesNew.keySet();
		for(Integer code : codes){
			int[] val = returncodesNew.get(code);
			if(!returncodes.containsKey(code)){
				returncodes.put(code, val);
			} else {
				returncodes.get(code)[0] += val[0];
			}
		}
	}
	
	/**
	 * Merges two excpetion maps
	 * 
	 * @param exceptions The original exceptions map that will be kept
	 * @param exceptionsNew The new exceptions map whose data will be merged into the original map
	 */
	protected void combineExceptionMaps(HashMap<String, Integer> exceptions, HashMap<String, Integer> exceptionsNew) {
		Set<String> keys = exceptionsNew.keySet();
		for(String key : keys){
			Integer valNew = exceptionsNew.get(key);
			Integer val = valNew;
			if(exceptions.containsKey(key)){
				val = exceptions.get(key) + valNew;
			}
			exceptions.put(key, val);
		}
	}
	
	/**
	 * Deep-clones a return code map
	 * 
	 * @param returncodes The return codes map to clone
	 * @return The clone of the return codes map
	 */
	protected HashMap<Integer, int[]> cloneReturnCodeMap(HashMap<Integer, int[]> returncodes){
		HashMap<Integer, int[]> clone = new HashMap<Integer, int[]>();
		Set<Integer> keySet = returncodes.keySet();
		for(int key : keySet){
			int[] value = returncodes.get(key);
			clone.put(key, value.clone());
		}
		return clone;
	}
	
	/**
	 * Deep-clones an exceptions map
	 * 
	 * @param exceptions The exceptions map to clone
	 * @return The clone of the exceptions map
	 */
	protected HashMap<String, Integer> cloneExceptionMap(HashMap<String, Integer> exceptions){
		HashMap<String, Integer> clone = new HashMap<String, Integer>();
		Set<String> keySet = exceptions.keySet();
		for(String key : keySet){
			Integer value = exceptions.get(key);
			clone.put(key, value);
		}
		return clone;
	}
}
