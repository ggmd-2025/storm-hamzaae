package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit5Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.SpeedBolt;

/**
 * Topologie T5 pour tester SpeedBolt avec calcul de vitesse moyenne
 * 
 * Flux: InputStreamSpout -> SpeedBolt -> Exit5Bolt
 * 
 * @author lumineau
 */
public class TopologyT5 {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopologyT5 <portINPUT> <portOUTPUT>");
            return;
        }
        
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        
        System.out.println("Starting TopologyT5:");
        System.out.println("  - Input port: " + portINPUT);
        System.out.println("  - Output port: " + portOUTPUT);
        
        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        
        /*Bolt qui calcule la vitesse moyenne avec fenêtre glissante*/
        builder.setBolt("speed", new SpeedBolt(), nbExecutors).shuffleGrouping("masterStream");
        
        /*Bolt qui émet le flux de sortie JSON*/
        builder.setBolt("exit", new Exit5Bolt(portOUTPUT), nbExecutors).shuffleGrouping("speed");
        
        /*Création d'une configuration*/
        Config config = new Config();
        config.setDebug(false); // Désactiver le debug pour réduire les logs
        
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT5", config, builder.createTopology());
        
        System.out.println("TopologyT5 submitted successfully!");
    }
}