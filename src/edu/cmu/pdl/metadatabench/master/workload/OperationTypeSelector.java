package edu.cmu.pdl.metadatabench.master.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.cmu.pdl.metadatabench.common.FileSystemOperationType;

/**
 * Provides functionality to select a random operation type from a set of operation types, according to a 
 * pre-defined discrete probability distribution.
 * 
 * @author emil.rakadjiev
 *
 */
public class OperationTypeSelector {

	private Random randomNumberGenerator;
	
	/** A list of the operation types */
	List<FileSystemOperationType> operationTypes;
	/** A list of cumulative operation probabilities, the indices in this list correspond the ones in the operation types map */
	List<Double> cumulativeOperationTypeProbabilities;
	
	/**
	 * @param operationTypeProbabilities @see edu.cmu.pdl.metadatabench.common.Config#getWorkloadOperationProbabilities()
	 */
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
	}
	
	/**
	 * Select a random operation type from a set of operation types, according to a pre-defined 
	 * discrete probability distribution.
	 * 
	 * @return The selected operation type
	 */
	public FileSystemOperationType getRandomOperationType(){
		// get a random double between 0 and 1
		double randomNumber = randomNumberGenerator.nextDouble();
		
		int i;
		// find the index in the cumulative probability list, which has a related probability higher than the random number
		for(i = 0 ; randomNumber > cumulativeOperationTypeProbabilities.get(i) ; i++);
		
		// use the index to get the corresponding operation type
		return operationTypes.get(i);
	}
	
}
