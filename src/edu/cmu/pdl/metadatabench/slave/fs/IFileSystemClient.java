package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;

/**
 * Client providing access to the underlying file system.
 * 
 * @author emil.rakadjiev
 *
 */
public interface IFileSystemClient {
	/**
	 * Creates a file in the underlying file system.
	 * @param path The path of the new file to create
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed
	 */
	public int create(String path) throws IOException;
	
	/**
	 * Deletes a directory or file in the underlying file system.
	 * 
	 * @param path The path of the directory or file to delete
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed, for example the directory or file is not found
	 */
	public int delete(String path) throws IOException;
	
	/**
	 * Lists the status of a directory or file in the underlying file system.
	 * @param path The path of the directory or file
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed, for example the directory or file is not found
	 */
	public int listStatus(String path) throws IOException;
	
	/**
	 * Creates a directory in the underlying file system.
	 * @param path The path of the new directory to create
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed
	 */
	public int mkdir(String path) throws IOException;
	
	/**
	 * Opens a file in the underlying file system.
	 * @param path The path of the directory or file
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed, for example the file is not found
	 */
	public int open(String path) throws IOException;
	
	/**
	 * Renames a directory or file in the underlying file system.
	 * @param fromPath The existing path of the directory or file
	 * @param toPath The new path of the directory or file
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed, for example the file is not found
	 */
	public int rename(String fromPath, String toPath) throws IOException;
	
	/**
	 * Moves a directory or file in the underlying file system.
	 * @param fromPath The existing path of the directory or file
	 * @param toPath The new path of the directory or file
	 * @return The latency of the operation
	 * @throws IOException If the operation could not be executed, for example the file is not found
	 */
	public int move(String fromPath, String toPath) throws IOException;
	
}
