package com.viva.extractor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Extractor {

	public static void main(String[] args) throws Exception {
		
		  String topicName=null;
		  String hostIP=null;
		  
		Properties props = new Properties();
		Logs log = new Logs();
		ConnectElasticSearch ces = new ConnectElasticSearch();
		
		FileInputStream in=null;
		try {
			in = new FileInputStream("src\\extractor.properties");
		} catch (FileNotFoundException e) {
		
			e.printStackTrace();
		}
   
    try {
		props.load(in);
	} catch (IOException e) {
	
		e.printStackTrace();
	}
        topicName=props.getProperty("topicName");
        hostIP=props.getProperty("hostIP");
		
		props.put("bootstrap.servers", hostIP);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

	
		consumer.subscribe(Arrays.asList(topicName));

		
		System.out.println("Reading from  " + topicName);
		int i = 0;

		List<ConsumerRecord<String, String>> lst = new ArrayList<ConsumerRecord<String, String>>();
		ArrayList<String> spl = new ArrayList<>();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				lst.add(record);
				String str = (lst.get(0).value());
				lst.clear();
				String[] arrOfStr = str.split("\\|", 0);
				for (String a : arrOfStr) {
					spl.add(a);
				}
				log.setTime_stamp(spl.get(0));
				log.setModule_name(spl.get(1));
				log.setHost_name(spl.get(2));
				log.setFile_name(spl.get(3));
				log.setFunction_name(spl.get(4));
				log.setLine_no(spl.get(5));
				log.setThread_no(spl.get(6));
				log.setMsg(spl.get(7));
				int res = ces.readLogs(log);
				System.out.println(res);
				spl.clear();
			}

		}
	}
}
