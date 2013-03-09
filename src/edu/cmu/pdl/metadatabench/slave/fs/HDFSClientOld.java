package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

/**
 * A client providing access to the underlying HDFS file system. This class uses the old HDFS API 
 * ({@link org.apache.hadoop.fs.FileSystem} instead of {@link org.apache.hadoop.fs.FileContext}).
 * 
 * @author emil.rakadjiev
 *
 */
public class HDFSClientOld implements IFileSystemClient {

	private FileSystem fileSystem;
	
	/**
	 * @param fileSystemAddress The address of the NameNode @see edu.cmu.pdl.metadatabench.common.Config#getFileSystemAddress()
	 */
	public HDFSClientOld(String fileSystemAddress){
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", fileSystemAddress);
			fileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			LoggerFactory.getLogger(HDFSClientOld.class).error("Cannot initialize the file system", e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int create(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		FSDataOutputStream out = fileSystem.create(new Path(path), true);
		out.close();
		return (int)(System.currentTimeMillis()-startTime);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int delete(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileSystem.delete(new Path(path), true);
		return (int)(System.currentTimeMillis()-startTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int listStatus(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileSystem.listStatus(new Path(path));
		return (int)(System.currentTimeMillis()-startTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int mkdir(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileSystem.mkdirs(new Path(path));
		return (int)(System.currentTimeMillis()-startTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int open(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		InputStream in = fileSystem.open(new Path(path));
		in.close();
	    return (int)(System.currentTimeMillis()-startTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int rename(String fromPath, String toPath) throws IOException{
		long startTime = System.currentTimeMillis();
		fileSystem.rename(new Path(fromPath), new Path(toPath));
		return (int)(System.currentTimeMillis()-startTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int move(String fromPath, String toPath) throws IOException {
		// HDFS has no explicit move operation
		return rename(fromPath, toPath);
	}

}
