package edu.cmu.pdl.metadatabench.slave.fs;

public interface IFileSystemClient {

	public long create(String path);

	public long delete(String path);

	public long listStatus(String path);

	public long mkdir(String path);

	public long open(String path);

	public long rename(String fromPath, String toPath);
	
}
