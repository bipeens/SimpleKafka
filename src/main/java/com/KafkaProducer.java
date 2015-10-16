package com;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer extends Thread{

	String TOPIC="test";
	
	public kafka.javaapi.producer.Producer<String, String> producer;
	
	public KafkaProducer(String topic)
	{
		this.TOPIC=topic;
		Properties properties=new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig producerConfig=new ProducerConfig(properties);
		producer=new kafka.javaapi.producer.Producer<String, String>(producerConfig);
	}
	
	@Override
	public void run()
	{
		String message = "\"point\":new GLatLng(40.266044,-74.718479), \"homeTeam\":\"Lawrence Library\",\"awayTeam\":\"LUGip\",\"markerImage\":\"images/red.png\",\"information\": \"Linux users group meets second Wednesday of each month.\",\"fixture\":\"Wednesday 7pm\",\"capacity\":\"\",\"previousScore\":\"\"";
		while(true)
		{
			SimpleDateFormat sdf = new SimpleDateFormat();
			KeyedMessage<String, String> msg =new KeyedMessage<String, String>(TOPIC,message + "  "+sdf.format(new Date()));
			producer.send(msg);
		}
	}

	public void closeProducer()
	{
		producer.close();
	}

}
