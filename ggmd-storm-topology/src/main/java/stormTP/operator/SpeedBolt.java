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
 * Opérateur stateless avec fenêtrage qui calcule la vitesse moyenne des tortues
 * Fenêtre glissante: 10 tops, calcul tous les 5 tuples reçus
 * Émet des tuples avec le schéma: (id, nom, tops, vitesse)
 */
public class SpeedBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107348L;
    private static Logger logger = Logger.getLogger("SpeedBoltLogger");
    private OutputCollector collector;
    
    // Configuration de la fenêtre glissante
    private static final int WINDOW_SIZE = 10; // 10 tops
    private static final int SLIDE_INTERVAL = 5; // tous les 5 tuples
    
    // Buffer pour stocker les données de fenêtrage par tortue
    private Map<Integer, TurtleWindow> turtleWindows = new HashMap<>();
    
    // Classe interne pour stocker les données de fenêtrage d'une tortue
    private static class TurtleWindow {
        int id;
        String nom;
        List<TurtleSnapshot> snapshots = new ArrayList<>();
        int tupleCount = 0;
        
        TurtleWindow(int id, String nom) {
            this.id = id;
            this.nom = nom;
        }
    }
    
    // Classe pour stocker un instantané d'une tortue à un top donné
    private static class TurtleSnapshot {
        int top;
        int nbCellsParcourus;
        
        TurtleSnapshot(int top, int nbCellsParcourus) {
            this.top = top;
            this.nbCellsParcourus = nbCellsParcourus;
        }
    }
    
    public SpeedBolt() {
    }
    
    @Override
    public void execute(Tuple t) {
        try {
            String jsonInput = t.getValueByField("json").toString();
            logger.info("Received JSON: " + jsonInput);
            
            // Parse le JSON et traite toutes les tortues
            parseJsonAndProcess(jsonInput, t);
            
        } catch (Exception e) {
            logger.severe("Error in SpeedBolt: " + e.getMessage());
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
                
                while (runnerMatcher.find()) {
                    String runnerJson = runnerMatcher.group();
                    
                    int id = extractIntField(runnerJson, "id");
                    int top = extractIntField(runnerJson, "top");
                    int tour = extractIntField(runnerJson, "tour");
                    int cellule = extractIntField(runnerJson, "cellule");
                    int maxcel = extractIntField(runnerJson, "maxcel");
                    
                    int nbCellsParcourus = tour * maxcel + cellule;
                    String nom = "Turtle" + id;
                    
                    // Traiter cette tortue pour le calcul de vitesse
                    processTurtleSpeed(id, nom, top, nbCellsParcourus, originalTuple);
                }
            }
            
            collector.ack(originalTuple);
            
        } catch (Exception e) {
            logger.severe("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
            collector.fail(originalTuple);
        }
    }
    
    private void processTurtleSpeed(int id, String nom, int top, int nbCellsParcourus, Tuple originalTuple) {
        // Initialiser la fenêtre de la tortue si nécessaire
        if (!turtleWindows.containsKey(id)) {
            turtleWindows.put(id, new TurtleWindow(id, nom));
        }
        
        TurtleWindow window = turtleWindows.get(id);
        
        // Ajouter le nouvel instantané
        window.snapshots.add(new TurtleSnapshot(top, nbCellsParcourus));
        window.tupleCount++;
        
        // Trier par top pour maintenir l'ordre chronologique
        Collections.sort(window.snapshots, new Comparator<TurtleSnapshot>() {
            @Override
            public int compare(TurtleSnapshot s1, TurtleSnapshot s2) {
                return Integer.compare(s1.top, s2.top);
            }
        });
        
        // Calculer la vitesse tous les SLIDE_INTERVAL tuples
        if (window.tupleCount % SLIDE_INTERVAL == 0 && window.snapshots.size() >= 2) {
            calculateAndEmitSpeed(window, originalTuple);
        }
        
        logger.info("Turtle id=" + id + " top=" + top + " cells=" + nbCellsParcourus + " tupleCount=" + window.tupleCount);
    }
    
    private void calculateAndEmitSpeed(TurtleWindow window, Tuple originalTuple) {
        List<TurtleSnapshot> snapshots = window.snapshots;
        
        // Prendre les WINDOW_SIZE derniers snapshots (ou tous s'il y en a moins)
        int startIndex = Math.max(0, snapshots.size() - WINDOW_SIZE);
        List<TurtleSnapshot> windowSnapshots = snapshots.subList(startIndex, snapshots.size());
        
        if (windowSnapshots.size() < 2) {
            return; // Pas assez de données pour calculer la vitesse
        }
        
        // Calculer la vitesse moyenne sur la fenêtre
        TurtleSnapshot first = windowSnapshots.get(0);
        TurtleSnapshot last = windowSnapshots.get(windowSnapshots.size() - 1);
        
        int topDiff = last.top - first.top;
        int cellDiff = last.nbCellsParcourus - first.nbCellsParcourus;
        
        if (topDiff > 0) {
            double vitesse = (double) cellDiff / topDiff;
            
            // Format de la chaîne tops
            String tops = first.top + "-" + last.top;
            
            logger.info("Emitting speed for turtle id=" + window.id + " tops=" + tops + " vitesse=" + vitesse);
            
            // Émettre le tuple avec le schéma (id, nom, tops, vitesse)
            collector.emit(originalTuple, new Values(window.id, window.nom, tops, vitesse));
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
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "nom", "tops", "vitesse"));
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