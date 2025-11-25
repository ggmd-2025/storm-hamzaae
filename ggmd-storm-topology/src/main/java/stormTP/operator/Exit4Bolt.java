package stormTP.operator;

import java.util.Map;
//import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.stream.StreamEmiter;

/**
 * Exit4Bolt basé sur ExitBolt
 * Prend en entrée des tuples de schéma (id, tops, score)
 * et produit en sortie un tuple de schéma (json)
 */
public class Exit4Bolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107347L;
    //private static Logger logger = Logger.getLogger("Exit4BoltLogger");
    private OutputCollector collector;
    int port = -1;
    StreamEmiter semit = null;
    
    public Exit4Bolt(int port) {
        this.port = port;
        this.semit = new StreamEmiter(this.port);
    }
    
    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
     */
    public void execute(Tuple t) {
        
        // Lire le schéma d'entrée: (id, tops, score)
        int id = (Integer) t.getValueByField("id");
        String tops = (String) t.getValueByField("tops");
        int score = (Integer) t.getValueByField("score");
        
        // Créer l'objet JSON attendu
        String jsonOutput = String.format("{\"id\":%d,\"tops\":\"%s\",\"score\":%d}", 
            id, tops, score);
        
        System.out.println("=== TURTLE BONUS SCORE ===");
        System.out.println(jsonOutput);
        
        // Émettre le tuple avec schéma (json) - comme dans ExitBolt original
        collector.emit(t, new Values(jsonOutput));
        
        // Envoyer aussi via StreamEmiter (attention au blocage potentiel)
        this.semit.send(jsonOutput);
        
        collector.ack(t);
        
        return;
    }
    
    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("json"));
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