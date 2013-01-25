package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;

public class HDFSClient implements IFileSystemClient {

	private FileContext fileContext;
	
	public HDFSClient(){
		try { // initialize file system handle
//			fileContext = FileContext.getFileContext(new Path("hdfs://localhost:9000").toUri());
			fileContext = FileContext.getFileContext(new Configuration());
		} catch (IOException ioe) {
			System.err.println("Can not initialize the file system: " + ioe.getLocalizedMessage());
		}
	}
	
	@Override
	public long create(String path){
		//TODO: delete created file afterwards?
		long startTime = System.currentTimeMillis();
		FSDataOutputStream out = null;
		try {
			out = fileContext.create(new Path(path), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
					CreateOpts.createParent(), CreateOpts.bufferSize(4096),	CreateOpts.repFac((short) 3));
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParentNotDirectoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long delete(String path){
		//TODO: what is the exact semantic of delete (create temp file/dir to delete it)?
		long startTime = System.currentTimeMillis();
		try {
			fileContext.delete(new Path(path), true);
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long listStatus(String path){
		long startTime = System.currentTimeMillis();
		try {
			fileContext.listStatus(new Path(path));
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long mkdir(String path){
		//TODO: delete created dir afterwards?
		long startTime = System.currentTimeMillis();
		try {
			fileContext.mkdir(new Path(path), FileContext.DEFAULT_PERM, true);
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParentNotDirectoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long open(String path){
		long startTime = System.currentTimeMillis();
		InputStream in = null;
		try {
			in = fileContext.open(new Path(path));
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return System.currentTimeMillis()-startTime;
	}
	
	@Override
	public long rename(String fromPath, String toPath){
		//TODO: what is the exact semantic of rename (create temp file/dir to rename it)?
		long startTime = System.currentTimeMillis();
		try {
			fileContext.rename(new Path(fromPath), new Path(toPath));
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParentNotDirectoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis()-startTime;
	}

}
