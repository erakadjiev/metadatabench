package edu.cmu.pdl.metadatabench.slave.fs;

import java.io.IOException;

/**
 * A dummy file system client, used for testing purposes. It does not access any file system, 
 * its methods just return a default value.
 * 
 * @author emil.rakadjiev
 *
 */
public class DummyClient implements IFileSystemClient {

	public DummyClient() {
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int create(String path) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int delete(String path) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int listStatus(String path) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int mkdir(String path) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int open(String path) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int rename(String fromPath, String toPath) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	/** Doesn't do anything, just returns 0. */
	@Override
	public int move(String fromPath, String toPath) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
