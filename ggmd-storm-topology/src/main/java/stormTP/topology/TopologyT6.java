package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit6Bolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.RankEvolutionBolt;

/**
 * Topologie T6 pour tester RankEvolutionBolt avec analyse d'évolution du rang
 * 
 * Flux: InputStreamSpout -> GiveRankBolt -> RankEvolutionBolt -> Exit6Bolt
 * Note: MyTortoiseBolt n'est pas utilisé car nous avons besoin du classement de toutes les tortues
 * 
 * @author lumineau
 */
public class TopologyT6 {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopologyT6 <portINPUT> <portOUTPUT> [targetTortoiseId]");
            return;
        }
        
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int targetId = 3; // default (pour MyTortoiseBolt si utilisé)
        if (args.length >= 3) {
            try {
                targetId = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid target id, using default 3");
            }
        }
        
        System.out.println("Starting TopologyT6:");
        System.out.println("  - Input port: " + portINPUT);
        System.out.println("  - Output port: " + portOUTPUT);
        System.out.println("  - Target turtle id: " + targetId + " (not used in this topology)");
        
        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        
        /*Bolt qui calcule les rangs de toutes les tortues*/
        builder.setBolt("giveRank", new GiveRankBolt(), nbExecutors).shuffleGrouping("masterStream");
        
        /*Bolt stateful qui analyse l'évolution du rang avec fenêtre temporelle*/
        builder.setBolt("rankEvolution", new RankEvolutionBolt(), nbExecutors).shuffleGrouping("giveRank");
        
        /*Bolt qui émet le flux de sortie JSON*/
        builder.setBolt("exit", new Exit6Bolt(portOUTPUT), nbExecutors).shuffleGrouping("rankEvolution");
        
        /*Création d'une configuration*/
        Config config = new Config();
        config.setDebug(false); // Désactiver le debug pour réduire les logs
        
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT6", config, builder.createTopology());
        
        System.out.println("TopologyT6 submitted successfully!");
    }
}