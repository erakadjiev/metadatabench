package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;

/**
 * A directory creation strategy that selects a parent directory from the existing set of directories 
 * using the Barabasi-Albert algorithm (see 
 * <a href="http://www3.nd.edu/~networks/Publication%20Categories/03%20Journal%20Articles/Physics/EmergenceRandom_Science%20286,%20509-512%20%281999%29.pdf">here</a> 
 * or <a href="http://en.wikipedia.org/wiki/Barabasi–Albert_model">here</a>).
 * The BA algorithm is used to generate scale-free (degree distribution follow a power law) random graphs. In 
 * each step, a new node is added using preferential attachment, that is the probability of selecting an existing 
 * node as the parent of the new one is proportional to the number of links (compared to the total number of links) 
 * that the existing node has.
 * Because the straightforward implementation of the BA algorithm is expensive, an optimized version is implemented here. 
 * In each step, an existing node (directory) is selected using a uniform distribution. Then either the selected 
 * node itself or its parent is selected (with equal probability) as the parent of the node to be created. 
 * 
 * @author emil.rakadjiev
 *
 */
public class BarabasiAlbertDirectoryCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private Random randomParent;

	/**
	 * @param dao The DAO to use for accessing the namespace map
	 * @param dispatcher The dispatcher used to send commands to other nodes
	 */
	public BarabasiAlbertDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher){
		super(dao, dispatcher);
		randomId = new Random();
		randomParent = new Random();
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The optimized Barabasi-Albert algorithm selects an existing directory using uniform distribution and then 
	 * chooses either the selected directory or its parent (with equal probability) and the parent of the directory 
	 * to be created.
	 */
	@Override
	public void createNextDirectory(int i){
		long parentId = selectParentDirectory(i);
		boolean parentsParent = randomParent.nextBoolean();
		String name = DIR_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(MKDIR_TYPE, parentId, parentsParent, i, name);
		dispatcher.dispatch(op);
	}
	
	/**
	 * Selects a parent directory from the set of existing directories using a uniform distribution.
	 * It is assumed that all directories with a sequence number lower than the sequence number i of 
	 * the directory to be created already exist. Thus, the id of the parent is a random number between 
	 * 1 and i-1
	 */
	public long selectParentDirectory(int i){
		int key = randomId.nextInt(i-1) + 1;
		return (long)key;
	}
	
}
