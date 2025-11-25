package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.ComputeBonusBolt;
import stormTP.operator.Exit4Bolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;

/**
 * Topologie T4 pour tester ComputeBonusBolt avec calcul de points bonus
 * 
 * Flux: InputStreamSpout -> MyTortoiseBolt -> GiveRankBolt -> ComputeBonusBolt -> Exit4Bolt
 * 
 * @author lumineau
 */
public class TopologyT4 {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopologyT4 <portINPUT> <portOUTPUT> [targetTortoiseId]");
            return;
        }
        
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int targetId = 3; // default
        if (args.length >= 3) {
            try {
                targetId = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid target id, using default 3");
            }
        }
        
        System.out.println("Starting TopologyT4:");
        System.out.println("  - Input port: " + portINPUT);
        System.out.println("  - Output port: " + portOUTPUT);
        System.out.println("  - Target turtle id: " + targetId);
        
        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        
        /*Bolt qui parse le JSON et filtre une tortue spécifique*/
        builder.setBolt("myTortoise", new MyTortoiseBolt(targetId), nbExecutors).shuffleGrouping("masterStream");
        
        /*Bolt qui calcule les rangs de toutes les tortues*/
        builder.setBolt("giveRank", new GiveRankBolt(), nbExecutors).shuffleGrouping("masterStream");
        
        /*Bolt stateful qui calcule les points bonus*/
        builder.setBolt("computeBonus", new ComputeBonusBolt(), nbExecutors).shuffleGrouping("giveRank");
        
        /*Bolt qui émet le flux de sortie JSON*/
        builder.setBolt("exit", new Exit4Bolt(portOUTPUT), nbExecutors).shuffleGrouping("computeBonus");
        
        /*Création d'une configuration*/
        Config config = new Config();
        config.setDebug(false); // Désactiver le debug pour réduire les logs
        
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
        
        System.out.println("TopologyT4 submitted successfully!");
    }
}