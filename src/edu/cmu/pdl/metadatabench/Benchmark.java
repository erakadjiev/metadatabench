package edu.cmu.pdl.metadatabench;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cmu.pdl.metadatabench.cluster.HazelcastCluster;
import edu.cmu.pdl.metadatabench.cluster.ICluster;
import edu.cmu.pdl.metadatabench.common.Config;
import edu.cmu.pdl.metadatabench.common.ConfigLoader;
import edu.cmu.pdl.metadatabench.master.Master;
import edu.cmu.pdl.metadatabench.slave.Slave;

public class Benchmark {

	// TODO: implement possibility to run multiple masters in parallel
	private static final int MASTERS = 1;
	
	private static final String OPT_MASTER = "master";
	private static final String OPT_SLAVES = "slaves";
	private static final String OPT_DIRS = "dirs";
	private static final String OPT_FILES = "files";
	private static final String OPT_OPS = "ops";
	private static final String OPT_SLAVE = "slave";
	private static final String OPT_FS_ADDRESS = "fs";
	private static final String OPT_STOP = "stop";
	private static final String OPT_CONFIG = "config";
	private static final String OPT_HELP = "help";
	
	private static final int SLEEP_TIME = 2000;
	
	private static class OptionComarator implements Comparator<Option> {

	    private static final List<String> OPTS_ORDER = new ArrayList<String>(9);
	    static{
	    	OPTS_ORDER.add(OPT_MASTER);
	    	OPTS_ORDER.add(OPT_SLAVES);
	    	OPTS_ORDER.add(OPT_DIRS);
	    	OPTS_ORDER.add(OPT_FILES);
	    	OPTS_ORDER.add(OPT_OPS);
	    	OPTS_ORDER.add(OPT_SLAVE);
	    	OPTS_ORDER.add(OPT_FS_ADDRESS);
	    	OPTS_ORDER.add(OPT_STOP);
	    	OPTS_ORDER.add(OPT_CONFIG);
	    	OPTS_ORDER.add(OPT_HELP);
	    }
	    
	    @Override
	    public int compare(Option o1, Option o2) {
	        return OPTS_ORDER.indexOf(o1.getOpt()) - OPTS_ORDER.indexOf(o2.getOpt());
	    }
	}
	
	public static void main(String[] args) {
		Logger log = LoggerFactory.getLogger(Benchmark.class);
		log.debug("Starting benchmark");
		Options options = initOptions();
		
		CommandLineParser cmd = new GnuParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = cmd.parse(options, args);
		} catch (ParseException e) {
			log.error("Error in command line parameters", e);
			printUsage(options);
			System.exit(0);
		}
		
		if(cmdLine != null){
			ICluster cluster = HazelcastCluster.getInstance();
			if(cmdLine.hasOption(OPT_CONFIG)){
				String configPath = cmdLine.getOptionValue(OPT_CONFIG);
				ConfigLoader.loadConfig(configPath);
			} else {
				ConfigLoader.loadConfig();
			}
			if(cmdLine.hasOption(OPT_MASTER)){
				int numberOfSlaves = 0;
				int numberOfDirs = 0;
				int numberOfFiles = 0;
				int numberOfOperations = 0;
				
				if(cmdLine.hasOption(OPT_SLAVES)){
					numberOfSlaves = Integer.parseInt(cmdLine.getOptionValue(OPT_SLAVES));
				} else {
					numberOfSlaves = Config.getNumberOfSlaves();
					if(numberOfSlaves < 1){
						log.error("Slaves parameter required for master");
						printUsage(options);
						System.exit(0);
					}
				}
				
				if(cmdLine.hasOption(OPT_DIRS)){
					numberOfDirs = Integer.parseInt(cmdLine.getOptionValue(OPT_DIRS));
				} else {
					numberOfDirs = Config.getNumberOfDirs();
					if(numberOfDirs < 1){
						log.error("Dirs parameter required for master");
						printUsage(options);
						System.exit(0);
					}
				}
				
				if(cmdLine.hasOption(OPT_FILES)){
					numberOfFiles = Integer.parseInt(cmdLine.getOptionValue(OPT_FILES));
				} else {
					numberOfFiles = Config.getNumberOfFiles();
				}
				
				if(cmdLine.hasOption(OPT_OPS)){
					numberOfOperations = Integer.parseInt(cmdLine.getOptionValue(OPT_OPS));
				} else {
					numberOfOperations = Config.getNumberOfOps();
				}
				
				cluster.joinAsMaster();
				
				try {
					log.debug("Going to sleep for {} ms after joining cluster.", SLEEP_TIME);
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					log.warn("Thread interrupted while sleeping", e);
				}
				
				log.info("Waiting for all members to join the cluster.");

				while(!cluster.allMembersJoined(MASTERS, numberOfSlaves)){
					try {
						log.debug("Going to sleep for {} ms while waiting for all memebers to join the cluster.", SLEEP_TIME);
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						log.warn("Thread interrupted while sleeping", e);
					}
				}
				
				log.info("All members joined the cluster. Starting the generation.");
				
				int id = cluster.generateMasterId();
				Master.start(((HazelcastCluster)cluster).getHazelcast(), id, MASTERS, numberOfDirs, numberOfFiles, numberOfOperations);
			} else if(cmdLine.hasOption(OPT_SLAVE)){
				
				String fsAddress = null;
				if(cmdLine.hasOption(OPT_FS_ADDRESS)){
					fsAddress = cmdLine.getOptionValue(OPT_FS_ADDRESS);
				} else {
					fsAddress = Config.getFileSystemAddress();
					if(fsAddress == null){
						log.error("Fs parameter required for slave");
						printUsage(options);
						System.exit(0);
					}
				}
				
				cluster.joinAsSlave();
				int id = cluster.generateSlaveId();
				Slave.start(((HazelcastCluster)cluster).getHazelcast(), id, fsAddress);
			} else if(cmdLine.hasOption(OPT_STOP)){
				cluster.stop();
			}
		} else {
			log.error("Invalid command line parameters");
			printUsage(options);
			System.exit(0);
		}
	}

	private static Options initOptions(){
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Starts a master (generator) node");
		Option master = OptionBuilder.create(OPT_MASTER); 
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("numberOfSlaves");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Number of slave nodes (master waits until all have joined the cluster). Applicable only for master.");
		Option slaves = OptionBuilder.create(OPT_SLAVES);
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("numberOfDirs");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Number of dirs to generate. Applicable only for master.");
		Option dirs = OptionBuilder.create(OPT_DIRS);
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("numberOfFiles");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Number of files to generate. Applicable only for master.");
		Option files = OptionBuilder.create(OPT_FILES);
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("numberOfOps");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Number of operations to generate. Applicable only for master.");
		Option ops = OptionBuilder.create(OPT_OPS);
		
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Starts a storage and worker node");
		Option slave = OptionBuilder.create(OPT_SLAVE); 
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("fileSystemAddress");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Address of the file system server. E.g. for HDFS this is the value of the fs.default.name " +
				"or fs.defaultFS parameter as defined in core-site.xml, using the format hdfs://host.name:port/.");
		Option fsAddress = OptionBuilder.create(OPT_FS_ADDRESS);
		
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Tries to stop all the nodes on the current machine. " +
				"Sometimes only manually killing of respective Java process helps.");
		Option stop = OptionBuilder.create(OPT_STOP); 
		
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Show this help");
		Option help = OptionBuilder.create(OPT_HELP); 
		
		OptionBuilder.hasArg();
		OptionBuilder.withArgName("file");
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Use the config properties file located at the given path");
		Option config = OptionBuilder.create(OPT_CONFIG); 
		
		OptionGroup optionGroup = new OptionGroup();
		optionGroup.addOption(master);
		optionGroup.addOption(slave);
		optionGroup.addOption(stop);
		optionGroup.addOption(help);
		optionGroup.setRequired(true);
		
		Options options = new Options();
		options.addOptionGroup(optionGroup);
		options.addOption(slaves);
		options.addOption(dirs);
		options.addOption(files);
		options.addOption(ops);
		options.addOption(fsAddress);
		options.addOption(config);
		
		return options;
	}
	
	private static void printUsage(Options options) {
		HelpFormatter help = new HelpFormatter();
		help.setOptionComparator(new OptionComarator());
		help.printHelp("\t\t\tmetadatabench -master -slaves NUM -dirs NUM [-files NUM] [-ops NUM] [-config PATH] or\nmetadatabench -slave -fs ADDR [-config PATH] or\nmetadatabench -stop or\nmetadatabench -help", options);
	}
	
}
