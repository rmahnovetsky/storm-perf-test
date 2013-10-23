package com.rpm.storm.payment.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.rpm.storm.payment.bolt.AckBolt;
import com.rpm.storm.payment.spout.SimpleSpout;

public class SpoutTestTopology2 {
   
	public static void main(String[] args) throws Exception {
        Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("SIMPLE_SPOUT", new SimpleSpout(), 20);

        builder.setBolt("ACK_BOLT", new AckBolt(), 20).shuffleGrouping("SIMPLE_SPOUT");

		if (args != null && args.length > 0) {
			config.setDebug(false);
			config.setNumAckers(20);
            config.setNumWorkers(4);
            config.setMaxTaskParallelism(60);
            config.setMaxSpoutPending(5000);
            config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
            
			StormSubmitter.submitTopology("SPOUT_TEST2", config, builder.createTopology());
		} else {
			config.setDebug(false);
            config.setNumWorkers(1);
            config.setNumAckers(0);
            config.setMaxTaskParallelism(60);
            config.setMaxSpoutPending(1000);
            config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
            config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
            config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("SPOUT_TEST", config, builder.createTopology());

			Thread.sleep(1000000);

			cluster.shutdown();
		}

	}
}
