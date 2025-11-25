package stormTP.operator;


import java.util.Map;
//import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;


public class Exit2Bolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	//private static Logger logger = Logger.getLogger("Exit2Bolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;
	
	public Exit2Bolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
	
		// Handle the new schema from MyTortoiseBolt: (id, top, nom, nbCellsParcourus, total, maxcel)
		try {
			int id = (Integer) t.getValueByField("id");
			int top = (Integer) t.getValueByField("top");
			String nom = (String) t.getValueByField("nom");
			int nbCellsParcourus = (Integer) t.getValueByField("nbCellsParcourus");
			int total = (Integer) t.getValueByField("total");
			int maxcel = (Integer) t.getValueByField("maxcel");
			
			// Format as JSON-like string for output
			String output = String.format("{\"id\":%d,\"top\":%d,\"nom\":\"%s\",\"nbCellsParcourus\":%d,\"total\":%d,\"maxcel\":%d}", 
				id, top, nom, nbCellsParcourus, total, maxcel);
			
			System.out.println("Exit2Bolt output: " + output);
			this.semit.send(output);
			collector.ack(t);
		} catch (Exception e) {
			System.err.println("Error in Exit2Bolt: " + e.getMessage());
			e.printStackTrace();
			collector.fail(t);
		}
		
		return;
	}
	

	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// Exit2Bolt doesn't emit tuples downstream, it sends to external stream
		// Keep original declaration for compatibility
		arg0.declare(new Fields("output"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}