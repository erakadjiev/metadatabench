package edu.cmu.pdl.metadatabench.generator;


public abstract class AbstractFileCreationStrategy {

	protected static char PATH_SEPARATOR = '/';
	private static String FILE_NAME_PREFIX = PATH_SEPARATOR + "file";
	
	protected int numberOfFiles;
	protected INamespaceMapDAO dao;
	
	public AbstractFileCreationStrategy(INamespaceMapDAO dao){
		numberOfFiles = 0;
		this.dao = dao;
	}
	
	abstract public String selectDirectory(long numberOfFiles);
	
	public void createNextFile(){
		String parentPath = selectDirectory(numberOfFiles);
		numberOfFiles++;
		String name = parentPath + FILE_NAME_PREFIX + numberOfFiles;
		dao.createFile(numberOfFiles, name);
	}
	
	public void testPrint(){
		System.out.println(dao.getNumberOfFiles());
		System.out.println(dao.getFile(1));
		System.out.println(dao.getFile(dao.getNumberOfFiles()/2));
	}
	
}
