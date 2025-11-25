package stormTP.operator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Opérateur stateful qui calcule le nombre de points bonus cumulés par les tortues.
 * Émet des tuples avec le schéma: (id, tops, score)
 * Les points bonus sont calculés tous les 15 tops.
 */
public class ComputeBonusBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107346L;
    private static Logger logger = Logger.getLogger("ComputeBonusBoltLogger");
    private OutputCollector collector;
    
    // État stateful : stockage des données par tortue
    private Map<Integer, TurtleState> turtleStates = new HashMap<>();
    
    // Constante pour le calcul des bonus
    private static final int BONUS_INTERVAL = 15;
    
    // Classe interne pour stocker l'état d'une tortue
    private static class TurtleState {
        int id;
        String nom;
        int totalScore = 0;
        int firstTop = -1;
        int lastTop = -1;
        List<RankData> rankHistory = new ArrayList<>();
        
        TurtleState(int id, String nom) {
            this.id = id;
            this.nom = nom;
        }
    }
    
    // Classe pour stocker les données de classement
    private static class RankData {
        int top;
        String rang;
        int total;
        
        RankData(int top, String rang, int total) {
            this.top = top;
            this.rang = rang;
            this.total = total;
        }
    }
    
    public ComputeBonusBolt() {
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
            
            logger.info("Received turtle: id=" + id + " top=" + top + " rang=" + rang);
            
            // Initialiser l'état de la tortue si nécessaire
            if (!turtleStates.containsKey(id)) {
                turtleStates.put(id, new TurtleState(id, "Turtle" + id));
            }
            
            TurtleState state = turtleStates.get(id);
            
            // Mettre à jour l'état
            if (state.firstTop == -1) {
                state.firstTop = top;
            }
            state.lastTop = top;
            
            // Ajouter les données de rang
            state.rankHistory.add(new RankData(top, rang, total));
            
            // Calculer les points bonus tous les 15 tops
            if (state.rankHistory.size() % BONUS_INTERVAL == 0) {
                int bonusPoints = calculateBonusPoints(state, total);
                state.totalScore += bonusPoints;
                
                // Créer la chaîne tops
                String tops = state.firstTop + "-" + state.lastTop;
                
                logger.info("Calculating bonus for turtle id=" + id + " tops=" + tops + " bonus=" + bonusPoints + " totalScore=" + state.totalScore);
                
                // Émettre le tuple avec le schéma (id, tops, score)
                collector.emit(t, new Values(id, tops, state.totalScore));
            }
            
            collector.ack(t);
            
        } catch (Exception e) {
            logger.severe("Error in ComputeBonusBolt: " + e.getMessage());
            e.printStackTrace();
            collector.fail(t);
        }
    }
    
    /**
     * Calcule les points bonus pour les 15 derniers tops
     */
    private int calculateBonusPoints(TurtleState state, int totalParticipants) {
        int bonusPoints = 0;
        
        // Prendre les 15 derniers classements
        int startIndex = Math.max(0, state.rankHistory.size() - BONUS_INTERVAL);
        List<RankData> recentRanks = state.rankHistory.subList(startIndex, state.rankHistory.size());
        
        for (RankData rankData : recentRanks) {
            int points = calculatePointsFromRank(rankData.rang, totalParticipants);
            bonusPoints += points;
            logger.info("Top " + rankData.top + " rang=" + rankData.rang + " points=" + points);
        }
        
        return bonusPoints;
    }
    
    /**
     * Convertit un rang en points bonus
     * Points = total participants - rang
     */
    private int calculatePointsFromRank(String rang, int totalParticipants) {
        try {
            // Extraire le nombre du rang (enlever "ex" si présent)
            String numericRank = rang.replaceAll("ex", "");
            int rankNumber = Integer.parseInt(numericRank);
            
            // Calculer les points : total participants - rang
            int points = totalParticipants - rankNumber;
            return Math.max(0, points); // Assurer que les points ne soient pas négatifs
            
        } catch (NumberFormatException e) {
            logger.warning("Invalid rank format: " + rang);
            return 0;
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "score"));
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