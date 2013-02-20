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
 */
@SuppressWarnings("serial")
public abstract class OneMeasurement implements Serializable, Cloneable {

	String _name;

	public String getName() {
		return _name;
	}

	/**
	 * @param _name
	 */
	public OneMeasurement(String _name) {
		this._name = _name;
	}

	public abstract void reportReturnCode(int code);

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
	
	public abstract void addMeasurement(OneMeasurement measurement);
	
	public OneMeasurement clone() throws CloneNotSupportedException {
		return (OneMeasurement) super.clone();
	}
	
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
	
	protected HashMap<Integer, int[]> cloneReturnCodeMap(HashMap<Integer, int[]> returncodes){
		HashMap<Integer, int[]> clone = new HashMap<Integer, int[]>();
		Set<Integer> keySet = returncodes.keySet();
		for(int key : keySet){
			int[] value = returncodes.get(key);
			clone.put(key, value.clone());
		}
		return clone;
	}
}
