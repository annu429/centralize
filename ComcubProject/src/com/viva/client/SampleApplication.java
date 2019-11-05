package com.viva.client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleApplication extends Thread {

	private static String fileName = null;
	public static String topicName = null;
	public static String broker_list = null;
	private final KafkaProducer<String, String> producer;
	private final Boolean isAsync;

	public SampleApplication(String topic, Boolean isAsync) {
		Properties props = new Properties();

		FileInputStream in = null;
		try {
			in = new FileInputStream("src\\application.properties");
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

		try {
			props.load(in);
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		fileName = props.getProperty("fileName");
		topicName = props.getProperty("topicName");
		broker_list = props.getProperty("hostIP");

		props.put("bootstrap.servers", broker_list);
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
		this.isAsync = isAsync;
	}

	public void sendMessage(String key, String value) {
		long startTime = System.currentTimeMillis();
		if (isAsync) { // Send asynchronously
			producer.send(new ProducerRecord<String, String>(topicName, key),
					(Callback) new MyCallBack(startTime, key, value));
		} else { // Send synchronously
			try {
				producer.send(new ProducerRecord<String, String>(topicName, key, value)).get();

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		SampleApplication producer = new SampleApplication(topicName, false);
		int lineCount = 0;
		FileInputStream fis;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(fileName);
			// Construct BufferedReader from InputStreamReader
			br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			while ((line = br.readLine()) != null) {
				lineCount++;
				producer.sendMessage(lineCount + "", line);
			}

		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}

	}
}

class MyCallBack implements Callback {

	private long startTime;
	private String key;
	private String message;

	public MyCallBack(long startTime, String key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {

		} else {
			exception.printStackTrace();
		}
	}
}