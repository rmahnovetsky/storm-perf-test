package com.rpm.storm.payment.spout;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Stopwatch;

public class SimpleSpout extends BaseRichSpout {
	private static final long serialVersionUID = 59040927882658334L;
	
	private static AtomicInteger count = new AtomicInteger(0);

	private static Stopwatch stopwatch = new Stopwatch();
	SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if(!stopwatch.isRunning()) stopwatch.start();
	}

	public void nextTuple() {
		this.collector.emit(new Values("1"), 1);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void ack(Object msgId) {
		count.incrementAndGet();
		if(count.get() % 1000 == 0) System.out.println(count.get() + " " + stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
	}
	
}
