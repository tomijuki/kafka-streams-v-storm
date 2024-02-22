package fer.hr.jukic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

    public static void main(String[] args) throws Exception{

        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(8);
        config.setNumEventLoggers(1);
        config.setDebug(true);
        config.setMaxTaskParallelism(1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new CallLogReaderSpout());
        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
                .shuffleGrouping("call-log-reader-spout");
        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
                .fieldsGrouping("call-log-creator-bolt", new Fields("call"));


        if(args!=null && args.length>0){
            try{
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e){
                e.printStackTrace();
            }
        } else {
            //only for testing, should use StormSubmitter instead, soon to be deprecated
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LogAnalyserTopology", config, builder.createTopology());
            Thread.sleep(40000);
            cluster.killTopology("LogAnalyserTopology");
            cluster.shutdown();
        }
    }
}