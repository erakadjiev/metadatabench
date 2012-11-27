package edu.cmu.pdl.metadatabench.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.security.AccessControlException;

public class HDFSClient {
	
	protected FileContext fileContext;
	
	public HDFSClient(){
		try { // initialize file system handle
			fileContext = FileContext.getFileContext(new Path("hdfs://localhost:9000").toUri());
//		fileContext = FileContext.getFileContext(new Configuration());
		} catch (IOException ioe) {
			System.err.println("Can not initialize the file system: " + ioe.getLocalizedMessage());
		}
	}
	
	public long create(String pathString){
		Path path = new Path(pathString);
		long startTime = System.currentTimeMillis();
		FSDataOutputStream out;
		try {
			out = fileContext.create(path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
					CreateOpts.createParent(), CreateOpts.bufferSize(4096),	CreateOpts.repFac((short) 3));
			out.close();
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
