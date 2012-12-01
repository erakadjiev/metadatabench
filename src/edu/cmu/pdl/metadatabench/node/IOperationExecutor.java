package edu.cmu.pdl.metadatabench.node;

public interface IOperationExecutor {

	public void create(String path);

	public void delete(String path);

	public void listStatus(String path);

	public void mkdir(String path);

	public void open(String path);

	public void rename(String fromPath, String toPath);
	
}
