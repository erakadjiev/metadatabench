package edu.cmu.pdl.metadatabench.cluster.communication;

import java.util.Collection;

import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsCollect;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.MeasurementsReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.NamespaceDelete;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressFinished;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReport;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.ProgressReset;
import edu.cmu.pdl.metadatabench.cluster.communication.messages.SimpleOperation;
import edu.cmu.pdl.metadatabench.measurement.MeasurementDataForNode;

/**
 * Dispatches messages and commands to nodes in the cluster.
 * 
 * @author emil.rakadjiev
 *
 */
public interface IDispatcher {

	/**
	 * Dispatches a {@link SimpleOperation}.
	 * 
	 * @param operation The operation to dispatch
	 */
	public void dispatch(SimpleOperation operation);
	
	/**
	 * Dispatches a {@link ProgressReport}.
	 * 
	 * @param report The progress report to dispatch
	 */
	public void dispatch(ProgressReport report);
	
	/**
	 * Dispatches a {@link ProgressReset}.
	 * 
	 * @param reset	The progress reset command to dispatch
	 */
	public void dispatch(ProgressReset reset);
	
	/**
	 * Dispatches a {@link ProgressFinish}.
	 * 
	 * @param finish	The progress finish command to dispatch
	 */
	public void dispatch(ProgressFinished finish);
	
	/**
	 * Dispatches a {@link MeasurementReset}.
	 * 
	 * @param reset	The measurement reset command to dispatch
	 */
	public void dispatch(MeasurementsReset reset);
	
	/**
	 * Dispatches a {@link NamespaceDelete}.
	 * 
	 * @param finish	The namespace deletion command to dispatch
	 * @return The time needed to delete the namespace
	 * @throws Exception If there is an error in the network connection or at the slave.
	 */
	public int dispatch(NamespaceDelete delete) throws Exception;
	
	/**
	 * Collects measurements from the slaves.
	 * 
	 * @param collectMeasurement The {@link MeasurementsCollect} command to dispatch
	 * @return A collection of measurements
	 * @throws Exception If there is an error in the network connection or at the slave.
	 */
	public Collection<MeasurementDataForNode> dispatch(MeasurementsCollect collectMeasurement) throws Exception;
	
}
