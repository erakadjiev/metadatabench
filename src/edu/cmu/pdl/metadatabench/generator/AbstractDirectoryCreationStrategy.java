package edu.cmu.pdl.metadatabench.generator;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public abstract class AbstractDirectoryCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String DIR_NAME_PREFIX = PATH_SEPARATOR + "dir";
	
	private String workingDirectory;
	protected int numberOfDirs;
	protected IMap<Integer,String> dirMap;
	
	public AbstractDirectoryCreationStrategy(String workingDirectory){
		this.workingDirectory = workingDirectory;
		while(this.workingDirectory.endsWith("/")){
			this.workingDirectory = this.workingDirectory.substring(0, this.workingDirectory.length() - 1);
		}
		numberOfDirs = 0;
		HazelcastInstance hci = Hazelcast.newHazelcastInstance(null); 
		dirMap = hci.getMap("directories");
	}
	
	abstract public String selectDirectory();
	
	public void createNextDirectory(){
		String parentPath = selectDirectory();
		numberOfDirs++;
		String name = parentPath + DIR_NAME_PREFIX + numberOfDirs;
		dirMap.put(numberOfDirs, name);
	}
	
	public void createRoot(){
		numberOfDirs++;
		String rootPath = workingDirectory + DIR_NAME_PREFIX + numberOfDirs;
		dirMap.put(numberOfDirs, rootPath);
		numberOfDirs++;
		String firstDirPath = rootPath + DIR_NAME_PREFIX + numberOfDirs;
		dirMap.put(numberOfDirs, firstDirPath);
	}
	
	public void testPrint(){
		System.out.println(dirMap.size());
		System.out.println(dirMap.get(1));
		System.out.println(dirMap.get(dirMap.size()/2));
	}
	
}
