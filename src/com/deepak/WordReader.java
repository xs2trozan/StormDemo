package com.deepak;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout{
	
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed  = false;
	private TopologyContext context;
	
	public boolean isDistributed(){return false;}
	

	public void ack(Object msgId) {
		System.out.println("OK:"+msgId); 
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId); 
		
	}

	public void nextTuple() {
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine())!= null){
				//Values value = new Values();
				//value.add(str);
				List<Object> a = new ArrayList();
				a.add(str);
				this.collector.emit(a);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			completed = true;
		}
			
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		String[] fields = {"line"};
		declarer.declare(new Fields(fields));
	}

	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
