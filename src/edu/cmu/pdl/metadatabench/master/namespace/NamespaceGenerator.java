package edu.cmu.pdl.metadatabench.master.namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.common.Config;


public class NamespaceGenerator {

	private AbstractDirectoryCreationStrategy dirCreator;
	private AbstractFileCreationStrategy fileCreator;
	private int id;
	private int masters;
	private Logger log;
	
	private static final int THROTTLE_AFTER_ITERATIONS = Config.getWorkloadThrottleAfterIterations();
	private static final int THROTTLE_DURATION = Config.getWorkloadThrottleDuration();
	
	public NamespaceGenerator(AbstractDirectoryCreationStrategy dirCreator, AbstractFileCreationStrategy fileCreator, int id, int masters){
		this.dirCreator = dirCreator;
		this.fileCreator = fileCreator;
		this.id = id;
		this.masters = masters;
		this.log = LoggerFactory.getLogger(NamespaceGenerator.class);
	}
	
	public void generateDirs(int numberOfDirs){
		if(id == 0){
			dirCreator.createRoot();
		}
		for(int i=2+id; i <= numberOfDirs; i+=masters){
			dirCreator.createNextDirectory(i);
			if((i % THROTTLE_AFTER_ITERATIONS) == 0){
				throttle();
			}
		}
	}
	
	public void generateFiles(int numberOfFiles){
		for(int i=1+id; i <= numberOfFiles; i+=masters){
			fileCreator.createNextFile(i);
			if((i % THROTTLE_AFTER_ITERATIONS) == 0){
				throttle();
			}
		}
	}
	
	private void throttle(){
		try {
			log.debug("Going to sleep for {} ms", THROTTLE_DURATION);
			Thread.sleep(THROTTLE_DURATION);
		} catch (InterruptedException e) {
			log.warn("Thread was interrupted while sleeping", e);
		}
	}

}
