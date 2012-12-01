package edu.cmu.pdl.metadatabench.node;

import java.util.concurrent.Callable;

public class CallableOperationFactory {

	private IOperationExecutor operationExecutor;
	
	public CallableOperationFactory(IOperationExecutor operationExecutor){
		this.operationExecutor = operationExecutor;
	}
	
	public Callable<Long> makeCreateCallable(final String path){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.create(path);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
	public Callable<Long> makeDeleteCallable(final String path){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.delete(path);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
	public Callable<Long> makeListStatusCallable(final String path){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.listStatus(path);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
	public Callable<Long> makeMkdirCallable(final String path){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.mkdir(path);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
	public Callable<Long> makeOpenCallable(final String path){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.open(path);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
	public Callable<Long> makeRenameCallable(final String fromPath, final String toPath){
		return new Callable<Long>(){
			@Override
			public Long call() throws Exception {
				operationExecutor.rename(fromPath, toPath);
				return 0L; // TODO: where to measure runtime?
			}
		};
	}
	
}
