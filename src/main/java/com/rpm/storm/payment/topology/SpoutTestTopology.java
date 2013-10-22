package com.rpm.storm.payment.topology;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.rpm.storm.payment.bolt.AckBolt;

public class SpoutTestTopology {
   
	public static void main(String[] args) throws Exception {
		
		OptionParser parser = new OptionParser() {
            {
                accepts( "zkConnect" ).withOptionalArg().ofType( String.class ).describedAs( "zookeeper: e.g. zookeeper:2181" ).defaultsTo( "localhost:2181" );
            }
        };
        parser.printHelpOn( System.out );
        
        OptionSet opts = parser.parse(args);
        Config config = new Config();
        
		SpoutConfig spoutConfig = new SpoutConfig(
				new SpoutConfig.ZkHosts((String)opts.valueOf("zkConnect"), "/brokers"),
				"test", // topic to read from
				"/kafkaTest", // the root path in Zookeeper for the spout to store the consumer offsets
				"discovery"); // an id for this consumer for storing the consumer offsets in Zookeeper
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
       
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("KAFKA_SPOUT", new KafkaSpout(spoutConfig), 20);

        builder.setBolt("ACK_BOLT", new AckBolt(), 20).shuffleGrouping("KAFKA_SPOUT");

		if (args != null && args.length > 0) {
			config.setDebug(false);
			config.setNumAckers(20);
            config.setNumWorkers(4);
            config.setMaxTaskParallelism(20);
            config.setMaxSpoutPending(5000);
            config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
            
			StormSubmitter.submitTopology("SPOUT_TEST", config, builder.createTopology());
		} else {
			config.setDebug(false);
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(1);
            config.setMaxSpoutPending(10);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("SPOUT_TEST", config, builder.createTopology());

			Thread.sleep(1000000);

			cluster.shutdown();
		}

	}
}
