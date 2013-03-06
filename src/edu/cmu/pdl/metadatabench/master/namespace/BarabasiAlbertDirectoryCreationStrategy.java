package edu.cmu.pdl.metadatabench.master.namespace;

import java.util.Random;

import edu.cmu.pdl.metadatabench.cluster.INamespaceMapDAO;
import edu.cmu.pdl.metadatabench.cluster.communication.IDispatcher;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.CreateOperation;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;

public class BarabasiAlbertDirectoryCreationStrategy extends AbstractDirectoryCreationStrategy {

	private Random randomId;
	private Random randomParent;
	private int masters;

	public BarabasiAlbertDirectoryCreationStrategy(INamespaceMapDAO dao, IDispatcher dispatcher, int masters){
		super(dao, dispatcher);
		randomId = new Random();
		randomParent = new Random();
		this.masters = masters;
	}
	
	@Override
	public void createNextDirectory(int i){
		long parentId = selectParentDirectory(i);
		boolean parentsParent = randomParent.nextBoolean();
		String name = DIR_NAME_PREFIX + i;
		SimpleOperation op = new CreateOperation(MKDIR_TYPE, parentId, parentsParent, i, name);
		dispatcher.dispatch(op);
	}
	
	public long selectParentDirectory(int i){
		int from = i;
		if(i > (masters)){
			from = i - ((i-1) % masters);
		}
		int key = randomId.nextInt(from-1) + 1;
		return (long)key;
	}
	
}
