package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit3Bolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.InputStreamSpout;

/**
 * Topologie T3 pour tester GiveRankBolt avec classement des tortues
 * 
 * Flux: InputStreamSpout -> GiveRankBolt -> Exit3Bolt
 * 
 * @author lumineau
 */
public class TopologyT3 {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopologyT3 <portINPUT> <portOUTPUT>");
            return;
        }
        
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        
        System.out.println("Starting TopologyT3:");
        System.out.println("  - Input port: " + portINPUT);
        System.out.println("  - Output port: " + portOUTPUT);
        
        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        
        /*Affectation du bolt qui calcule les rangs, il prend en input le spout masterStream*/
        builder.setBolt("giveRank", new GiveRankBolt(), nbExecutors).shuffleGrouping("masterStream");
        
        /*Affectation du bolt qui émet le flux de sortie JSON, il prend en input le bolt giveRank*/
        builder.setBolt("exit", new Exit3Bolt(portOUTPUT), nbExecutors).shuffleGrouping("giveRank");
        
        /*Création d'une configuration*/
        Config config = new Config();
        config.setDebug(false); // Désactiver le debug pour réduire les logs
        
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
        
        System.out.println("TopologyT3 submitted successfully!");
    }
}