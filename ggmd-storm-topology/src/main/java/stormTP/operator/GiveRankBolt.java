package stormTP.operator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * Bolt qui détermine le classement des tortues sur la piste
 * Émet des tuples avec le schéma: (id, top, rang, total, maxcel)
 */
public class GiveRankBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107344L;
    private static Logger logger = Logger.getLogger("GiveRankBoltLogger");
    private OutputCollector collector;
    
    // Stockage des données des tortues pour un top donné
    private Map<Integer, Map<Integer, TurtleData>> topToTurtles = new HashMap<>();
    
    // Classe interne pour stocker les données d'une tortue
    private static class TurtleData {
        int id;
        int top;
        String nom;
        int nbCellsParcourus;
        int total;
        int maxcel;
        Tuple originalTuple;
        
        TurtleData(int id, int top, String nom, int nbCellsParcourus, int total, int maxcel, Tuple tuple) {
            this.id = id;
            this.top = top;
            this.nom = nom;
            this.nbCellsParcourus = nbCellsParcourus;
            this.total = total;
            this.maxcel = maxcel;
            this.originalTuple = tuple;
        }
    }
    
    public GiveRankBolt() {
    }
    
    @Override
    public void execute(Tuple t) {
        try {
            // Parse JSON input from InputStreamSpout
            String jsonInput = t.getValueByField("json").toString();
            logger.info("Received JSON: " + jsonInput);
            
            // Parse the JSON to extract runners
            parseJsonAndProcess(jsonInput, t);

            
        } catch (Exception e) {
            logger.severe("Error in GiveRankBolt: " + e.getMessage());
            e.printStackTrace();
            collector.fail(t);
        }
    }
    
    private void parseJsonAndProcess(String jsonInput, Tuple originalTuple) {
        try {
            // Find all runners in the JSON
            Pattern runnersPattern = Pattern.compile("\"runners\"\\s*:\\s*\\[([^\\]]+)\\]");
            Matcher runnersMatcher = runnersPattern.matcher(jsonInput);
            
            if (runnersMatcher.find()) {
                String runnersArray = runnersMatcher.group(1);
                
                // Parse individual runner objects
                Pattern runnerPattern = Pattern.compile("\\{[^}]+\\}");
                Matcher runnerMatcher = runnerPattern.matcher(runnersArray);
                
                List<TurtleData> turtlesInThisFrame = new ArrayList<>();
                int total = 0;
                int maxcel = 0;
                int top = 0;
                
                while (runnerMatcher.find()) {
                    String runnerJson = runnerMatcher.group();
                    
                    int id = extractIntField(runnerJson, "id");
                    top = extractIntField(runnerJson, "top"); // Get top from individual runner
                    int tour = extractIntField(runnerJson, "tour");
                    int cellule = extractIntField(runnerJson, "cellule");
                    total = extractIntField(runnerJson, "total");
                    maxcel = extractIntField(runnerJson, "maxcel");
                    
                    int nbCellsParcourus = tour * maxcel + cellule;
                    String nom = "Turtle" + id; // Simple name generation
                    
                    turtlesInThisFrame.add(new TurtleData(id, top, nom, nbCellsParcourus, total, maxcel, originalTuple));
                }
                
                // Calculate ranks for all turtles and emit
                if (!turtlesInThisFrame.isEmpty()) {
                    calculateRanksAndEmitAll(turtlesInThisFrame);
                }
            }
            
        } catch (Exception e) {
            logger.severe("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
            collector.fail(originalTuple);
        }
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
    
    private void calculateRanksAndEmit(int top, Map<Integer, TurtleData> turtles) {
        // Convertir en liste pour le tri
        List<TurtleData> turtleList = new ArrayList<>(turtles.values());
        calculateRanksAndEmitAll(turtleList);
    }
    
    private void calculateRanksAndEmitAll(List<TurtleData> turtleList) {
        logger.info("Processing " + turtleList.size() + " turtles for ranking");
        
        // Trier par nbCellsParcourus (décroissant)
        Collections.sort(turtleList, new Comparator<TurtleData>() {
            @Override
            public int compare(TurtleData t1, TurtleData t2) {
                return Integer.compare(t2.nbCellsParcourus, t1.nbCellsParcourus);
            }
        });
        
        // Log sorted results
        for (int i = 0; i < turtleList.size(); i++) {
            TurtleData turtle = turtleList.get(i);
            logger.info("Sorted position " + i + ": id=" + turtle.id + " cells=" + turtle.nbCellsParcourus);
        }
        
        // Calculer les rangs
        for (int i = 0; i < turtleList.size(); i++) {
            TurtleData turtle = turtleList.get(i);
            String rang = calculateRank(i, turtle, turtleList);
            
            logger.info("Emitting turtle id=" + turtle.id + " rang=" + rang + " cells=" + turtle.nbCellsParcourus);
            collector.emit(turtle.originalTuple, new Values(turtle.id, turtle.top, rang, turtle.total, turtle.maxcel));
        }
        
        // Ack the original tuple once for all emissions
        if (!turtleList.isEmpty()) {
            collector.ack(turtleList.get(0).originalTuple);
        }
    }
    
    private String calculateRank(int position, TurtleData turtle, List<TurtleData> sortedTurtles) {
        // Find the actual rank based on unique scores
        int actualRank = 1;
        for (int i = 0; i < position; i++) {
            if (sortedTurtles.get(i).nbCellsParcourus > turtle.nbCellsParcourus) {
                actualRank++;
            }
        }
        
        // Check if there are ties
        boolean hasEquality = false;
        for (int i = 0; i < sortedTurtles.size(); i++) {
            if (i != position && sortedTurtles.get(i).nbCellsParcourus == turtle.nbCellsParcourus) {
                hasEquality = true;
                break;
            }
        }
        
        logger.info("Turtle id=" + turtle.id + " position=" + position + " actualRank=" + actualRank + " hasEquality=" + hasEquality);
        
        // Return rank with "ex" if there are ties
        if (hasEquality) {
            return actualRank + "ex";
        } else {
            return String.valueOf(actualRank);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    @Override
    public void cleanup() {
    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
}