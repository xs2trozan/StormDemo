package com.deepak;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {
	
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	@Override
	public void cleanup() {
		System.out.println("--Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	@Override
	public void execute(Tuple input) {
		List<Object> inputTupleList = input.getValues();
 		
		String str = inputTupleList.get(0).toString();
		
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<String,Integer>();
		this.collector = collector;
		this.name =  context.getThisComponentId();
		this.id = context.getThisTaskId();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
