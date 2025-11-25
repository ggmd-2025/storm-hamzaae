package stormTP.operator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Opérateur stateful avec fenêtrage temporel qui détermine l'évolution du rang des tortues.
 * Fenêtre temporelle: 30 secondes
 * Émet des tuples avec le schéma: (id, nom, date, evolution)
 */
public class RankEvolutionBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107350L;
    private static Logger logger = Logger.getLogger("RankEvolutionBoltLogger");
    private OutputCollector collector;
    
    // Configuration de la fenêtre temporelle
    private static final long WINDOW_DURATION_MS = 30000; // 30 secondes
    
    // Stockage des données de rang par tortue avec fenêtre temporelle
    private Map<Integer, TurtleRankHistory> turtleHistories = new HashMap<>();
    
    // Classe interne pour stocker l'historique des rangs d'une tortue
    private static class TurtleRankHistory {
        int id;
        String nom;
        List<RankEntry> rankEntries = new ArrayList<>();
        long lastCalculationTime = 0;
        
        TurtleRankHistory(int id, String nom) {
            this.id = id;
            this.nom = nom;
        }
    }
    
    // Classe pour stocker une entrée de rang avec timestamp
    private static class RankEntry {
        long timestamp;
        int top;
        String rang;
        int numericRank;
        
        RankEntry(long timestamp, int top, String rang) {
            this.timestamp = timestamp;
            this.top = top;
            this.rang = rang;
            this.numericRank = parseNumericRank(rang);
        }
        
        private static int parseNumericRank(String rang) {
            try {
                return Integer.parseInt(rang.replaceAll("ex", ""));
            } catch (NumberFormatException e) {
                return Integer.MAX_VALUE; // Rang invalide -> dernier
            }
        }
    }
    
    public RankEvolutionBolt() {
    }
    
    @Override
    public void execute(Tuple t) {
        try {
            // Lire le schéma d'entrée de GiveRankBolt: (id, top, rang, total, maxcel)
            int id = (Integer) t.getValueByField("id");
            int top = (Integer) t.getValueByField("top");
            String rang = (String) t.getValueByField("rang");
            int total = (Integer) t.getValueByField("total");
            int maxcel = (Integer) t.getValueByField("maxcel");
            
            long currentTime = System.currentTimeMillis();
            String nom = "Turtle" + id;
            
            logger.info("Received turtle: id=" + id + " top=" + top + " rang=" + rang);
            
            // Initialiser l'historique de la tortue si nécessaire
            if (!turtleHistories.containsKey(id)) {
                turtleHistories.put(id, new TurtleRankHistory(id, nom));
            }
            
            TurtleRankHistory history = turtleHistories.get(id);
            
            // Ajouter la nouvelle entrée de rang
            history.rankEntries.add(new RankEntry(currentTime, top, rang));
            
            // Nettoyer les entrées trop anciennes (hors fenêtre)
            cleanOldEntries(history, currentTime);
            
            // Calculer l'évolution si assez de temps s'est écoulé depuis le dernier calcul
            if (currentTime - history.lastCalculationTime >= WINDOW_DURATION_MS) {
                String evolution = calculateRankEvolution(history, currentTime);
                if (evolution != null) {
                    // Formater la date
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String date = sdf.format(new Date(currentTime));
                    
                    logger.info("Emitting evolution for turtle id=" + id + " evolution=" + evolution);
                    
                    // Émettre le tuple avec le schéma (id, nom, date, evolution)
                    collector.emit(t, new Values(id, nom, date, evolution));
                    
                    history.lastCalculationTime = currentTime;
                }
            }
            
            collector.ack(t);
            
        } catch (Exception e) {
            logger.severe("Error in RankEvolutionBolt: " + e.getMessage());
            e.printStackTrace();
            collector.fail(t);
        }
    }
    
    /**
     * Nettoie les entrées trop anciennes (hors de la fenêtre temporelle)
     */
    private void cleanOldEntries(TurtleRankHistory history, long currentTime) {
        Iterator<RankEntry> iterator = history.rankEntries.iterator();
        while (iterator.hasNext()) {
            RankEntry entry = iterator.next();
            if (currentTime - entry.timestamp > WINDOW_DURATION_MS) {
                iterator.remove();
            }
        }
    }
    
    /**
     * Calcule l'évolution du rang sur la fenêtre temporelle
     */
    private String calculateRankEvolution(TurtleRankHistory history, long currentTime) {
        if (history.rankEntries.size() < 2) {
            return null; // Pas assez de données
        }
        
        // Trouver la première et la dernière entrée dans la fenêtre
        RankEntry firstEntry = null;
        RankEntry lastEntry = null;
        
        for (RankEntry entry : history.rankEntries) {
            if (currentTime - entry.timestamp <= WINDOW_DURATION_MS) {
                if (firstEntry == null || entry.timestamp < firstEntry.timestamp) {
                    firstEntry = entry;
                }
                if (lastEntry == null || entry.timestamp > lastEntry.timestamp) {
                    lastEntry = entry;
                }
            }
        }
        
        if (firstEntry == null || lastEntry == null || firstEntry == lastEntry) {
            return null;
        }
        
        // Calculer l'évolution
        int rankDifference = firstEntry.numericRank - lastEntry.numericRank;
        
        logger.info("Turtle id=" + history.id + " rank evolution: " + firstEntry.numericRank + " -> " + lastEntry.numericRank + " (diff=" + rankDifference + ")");
        
        if (rankDifference > 0) {
            return "En progression"; // Le rang a diminué (meilleur classement)
        } else if (rankDifference < 0) {
            return "En régression"; // Le rang a augmenté (pire classement)
        } else {
            return "Constant"; // Même rang
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "nom", "date", "evolution"));
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