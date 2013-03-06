package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

public class HDFSClient implements IFileSystemClient {

	private FileContext fileContext;
	
	public HDFSClient(String fileSystemAddress){
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", fileSystemAddress);
			fileContext = FileContext.getFileContext(conf);
		} catch (IOException e) {
			LoggerFactory.getLogger(HDFSClient.class).error("Cannot initialize the file system", e);
		}
	}
	
	@Override
	public int create(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		FSDataOutputStream out = fileContext.create(new Path(path), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
		out.close();
		return (int)(System.currentTimeMillis()-startTime);
	}
	
	@Override
	public int delete(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.delete(new Path(path), true);
		return (int)(System.currentTimeMillis()-startTime);
	}
	
	@Override
	public int listStatus(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.listStatus(new Path(path));
		return (int)(System.currentTimeMillis()-startTime);
	}
	
	@Override
	public int mkdir(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.mkdir(new Path(path), FileContext.DEFAULT_PERM, true);
		return (int)(System.currentTimeMillis()-startTime);
	}
	
	@Override
	public int open(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		InputStream in = fileContext.open(new Path(path));
		in.close();
	    return (int)(System.currentTimeMillis()-startTime);
	}
	
	@Override
	public int rename(String fromPath, String toPath) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.rename(new Path(fromPath), new Path(toPath));
		return (int)(System.currentTimeMillis()-startTime);
	}

	@Override
	public int move(String fromPath, String toPath) throws IOException {
		// HDFS has no explicit move operation
		return rename(fromPath, toPath);
	}

}
