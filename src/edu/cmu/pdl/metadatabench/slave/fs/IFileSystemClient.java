package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;

public interface IFileSystemClient {

	public long create(String path) throws IOException;

	public long delete(String path) throws IOException;

	public long listStatus(String path) throws IOException;

	public long mkdir(String path) throws IOException;

	public long open(String path) throws IOException;

	public long rename(String fromPath, String toPath) throws IOException;
	
}
