package com.rpm.storm.payment.bolt;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AckBolt implements Serializable, IRichBolt {
	private static final long serialVersionUID = -2764901229599142676L;
	
	private OutputCollector collector;
	//private static AtomicInteger count = new AtomicInteger(0);

	//private static Stopwatch stopwatch = new Stopwatch();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		//if(!stopwatch.isRunning()) stopwatch.start();
	}

	
	@Override
	public void execute(Tuple input) {
		collector.emit(input, new Values("1"));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}