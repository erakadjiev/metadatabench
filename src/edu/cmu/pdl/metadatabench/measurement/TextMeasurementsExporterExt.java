package edu.cmu.pdl.metadatabench.measurement;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class TextMeasurementsExporterExt implements MeasurementsExporterExt {

	private BufferedWriter bw;

	public TextMeasurementsExporterExt(OutputStream os) {
		this.bw = new BufferedWriter(new OutputStreamWriter(os));
	}

	public void write(String metric, String measurement, int i)	throws IOException {
		bw.write("[" + metric + "], " + measurement + ", " + i);
		bw.newLine();
	}

	public void write(String metric, String measurement, double d) throws IOException {
		bw.write("[" + metric + "], " + measurement + ", " + d);
		bw.newLine();
	}
	
	public void write(String text) throws IOException {
		bw.write(text);
		bw.newLine();
	}

	public void close() throws IOException {
		this.bw.close();
	}

}
