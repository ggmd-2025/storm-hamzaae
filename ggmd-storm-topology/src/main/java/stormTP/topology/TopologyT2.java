package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit2Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;

/**
 * 
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input 
 *
 */
public class TopologyT2 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
        if (args.length < 2) {
            System.err.println("Usage: TopologyT2 <portINPUT> <portOUTPUT> [targetTortoiseId]");
            return;
        }
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int targetId = 3; // default
        if (args.length >= 3) {
            try {
                targetId = Integer.parseInt(args[2]);
                System.out.println("========>Target tortoise id set to " + targetId);
            } catch (NumberFormatException e) {
                System.err.println("Invalid target id, using default 3");
            }
        }
    	
		/*Création du spout*/
    	InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
    	/*Création de la topologie*/
    	TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        /*Affectation à la topologie */
        builder.setBolt("MyTortoiseBolt", new MyTortoiseBolt(targetId), nbExecutors).shuffleGrouping("masterStream");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new Exit2Bolt(portOUTPUT), nbExecutors).shuffleGrouping("MyTortoiseBolt");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
	}
		
	
}