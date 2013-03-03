package edu.cmu.pdl.metadatabench.cluster;

public interface INamespaceMapDAO {

	public void createDir(long id, String path);
	public String getDir(long id);
	public void deleteDir(long id);
	public long getNumberOfDirs();
	
	public void createFile(long id, String path);
	public String getFile(long id);
	public void deleteFile(long id);
	public void renameFile(long id, String pathNew);
	public long getNumberOfFiles();
	
}
