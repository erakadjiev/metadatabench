package edu.cmu.pdl.metadatabench.measurement;

import java.io.IOException;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

public interface MeasurementsExporterExt extends MeasurementsExporter {

	/**
	   * Optional method for writing some text (header, message, etc.) to the exported format.
	   * 
	   * @param text Text to write.
	   * @throws IOException if writing failed
	   */
	  public void write(String text) throws IOException;
	
}
