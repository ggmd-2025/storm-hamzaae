package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class MyTortoiseBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
	private OutputCollector collector;
	private int targetId = 3; // default target tortoise id
	private static final String[] NAMES = new String[]{"Caroline", "Donatello", "Raphaelo", "Michelangelo", "Gamera", "Leonardo", "April"};
	
	
	public MyTortoiseBolt () {
		this(3);
	}

	/**
	 * Constructeur permettant de cibler une tortue précise (par id)
	 * @param targetId id de la tortue à filtrer
	 */
	public MyTortoiseBolt(int targetId) {
		this.targetId = targetId;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {

		try {
			String n = t.getValueByField("json").toString();
			logger.info("================================================================================");
			logger.info("=> Processing JSON: " + n);
			
			// Simple regex-based parsing for the specific JSON structure
			// Looking for runner objects with "id":<targetId>
			Pattern runnerPattern = Pattern.compile("\\{[^}]*\"id\"\\s*:\\s*" + this.targetId + "[^}]*\\}");
			Matcher matcher = runnerPattern.matcher(n);
			
			if (matcher.find()) {
				String runnerJson = matcher.group();
				logger.info("=> Found target runner: " + runnerJson);
				
				// Extract individual fields using regex
				int id = extractIntField(runnerJson, "id");
				int top = extractIntField(runnerJson, "top");
				int tour = extractIntField(runnerJson, "tour");
				int cellule = extractIntField(runnerJson, "cellule");
				int total = extractIntField(runnerJson, "total");
				int maxcel = extractIntField(runnerJson, "maxcel");
				
				int nbCellsParcourus = tour * maxcel + cellule;
				String nom = NAMES[id % NAMES.length];
				
				logger.info("=> turtle id=" + id + " top=" + top + " name=" + nom + " cells=" + nbCellsParcourus);
				collector.emit(t, new Values(id, top, nom, nbCellsParcourus, total, maxcel));
				collector.ack(t);
				return;
			}
			
			// target not found in this frame
			logger.info("=> Target turtle id=" + this.targetId + " not found in this frame");
			collector.ack(t);
		} catch (Exception e) {
			logger.severe("Error parsing tuple: " + e.getMessage());
			e.printStackTrace();
			collector.fail(t);
		}
		return;
	}
	
	/**
	 * Extract integer field value from JSON string using regex
	 */
	private int extractIntField(String json, String fieldName) {
		Pattern pattern = Pattern.compile("\"" + fieldName + "\"\\s*:\\s*(\\d+)");
		Matcher matcher = pattern.matcher(json);
		if (matcher.find()) {
			return Integer.parseInt(matcher.group(1));
		}
		throw new RuntimeException("Field " + fieldName + " not found in JSON: " + json);
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
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