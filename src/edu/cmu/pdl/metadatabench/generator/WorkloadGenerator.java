package edu.cmu.pdl.metadatabench.generator;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGenerator {

	private static int NUM_OPS = 100000; //TODO: param
	public static final Map<FileSystemOperationType,Double> OPERATION_PROBABILITIES = new HashMap<FileSystemOperationType,Double>();
	static{
		OPERATION_PROBABILITIES.put(FileSystemOperationType.CREATE, 0.2);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.LIST_STATUS, 0.4);
		OPERATION_PROBABILITIES.put(FileSystemOperationType.OPEN, 0.4);
	}
	
	private OperationTypeSelector operationTypeSelector;
	private DirectorySelector directorySelector;
	
	public WorkloadGenerator(){
		operationTypeSelector = new OperationTypeSelector(OPERATION_PROBABILITIES);
		directorySelector = new DirectorySelector();
	}
	
	public void generate(){
		for(int i=0; i<NUM_OPS; i++){
			FileSystemOperationType operation = operationTypeSelector.getRandomOperationType();
		}
	}
	
}
