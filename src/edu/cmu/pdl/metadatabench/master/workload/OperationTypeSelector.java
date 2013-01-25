package edu.cmu.pdl.metadatabench.master.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.cmu.pdl.metadatabench.cluster.FileSystemOperationType;

public class OperationTypeSelector {

	private Random randomNumberGenerator;
	
	List<FileSystemOperationType> operationTypes;
	List<Double> cumulativeOperationTypeProbabilities;
	
	public OperationTypeSelector(Map<FileSystemOperationType,Double> operationTypeProbabilities){
		//TODO: use custom seed?
		randomNumberGenerator = new Random();
		
		Set<FileSystemOperationType> operationTypesSet = operationTypeProbabilities.keySet();
		int numberOfOperationTypes = operationTypesSet.size();
		
		operationTypes = new ArrayList<FileSystemOperationType>(numberOfOperationTypes);
		cumulativeOperationTypeProbabilities = new ArrayList<Double>(numberOfOperationTypes);
		double sumOfProbabilities = 0;
		
		for(FileSystemOperationType operationType : operationTypesSet){
			operationTypes.add(operationType);
			double operationProbability = operationTypeProbabilities.get(operationType);
			sumOfProbabilities += operationProbability;
			cumulativeOperationTypeProbabilities.add(sumOfProbabilities);
		}
		//TODO: check if probabilities add up to 1 (or interpret them as weights)
	}
	
	public FileSystemOperationType getRandomOperationType(){
		double randomNumber = randomNumberGenerator.nextDouble();
		
		int i;
		for(i = 0 ; randomNumber > cumulativeOperationTypeProbabilities.get(i) ; i++);
		
		return operationTypes.get(i);
	}
	
}
