package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;

public interface IFileSystemClient {

	public int create(String path) throws IOException;
	public int delete(String path) throws IOException;
	public int listStatus(String path) throws IOException;
	public int mkdir(String path) throws IOException;
	public int open(String path) throws IOException;
	public int rename(String fromPath, String toPath) throws IOException;
	public int move(String fromPath, String toPath) throws IOException;
	
}
