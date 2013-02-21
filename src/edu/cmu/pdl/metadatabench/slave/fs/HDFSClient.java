package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

public class HDFSClient implements IFileSystemClient {

	private FileContext fileContext;
	
	public HDFSClient(){
		try {
//			fileContext = FileContext.getFileContext(new Path("hdfs://localhost:9000").toUri());
			fileContext = FileContext.getFileContext(new Configuration());
		} catch (IOException ioe) {
			System.err.println("Can not initialize the file system: " + ioe.getLocalizedMessage());
		}
	}
	
	@Override
	public long create(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		FSDataOutputStream out = fileContext.create(new Path(path), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE));
		out.close();
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long delete(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.delete(new Path(path), true);
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long listStatus(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.listStatus(new Path(path));
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long mkdir(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.mkdir(new Path(path), FileContext.DEFAULT_PERM, true);
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long open(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		InputStream in = fileContext.open(new Path(path));
		in.close();
	    return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long rename(String fromPath, String toPath) throws IOException{
		long startTime = System.currentTimeMillis();
		fileContext.rename(new Path(fromPath), new Path(toPath));
		return System.currentTimeMillis()-startTime;
	}

	@Override
	public long move(String fromPath, String toPath) throws IOException {
		throw new UnsupportedOperationException("HDFS has no explicit move operation. Use rename instead.");
	}

}
