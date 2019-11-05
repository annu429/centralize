package com.viva.extractor;
import scala.util.control.Exception.Catch;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

public class ConnectElasticSearch {

	public static int readLogs(Logs log) throws ClientProtocolException, IOException {
		String url;
		String port;
		String index;
		String type;
		Properties props = new Properties();
		FileInputStream in = null;
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

		url = props.getProperty("elastic_url");
		port = props.getProperty("port");
		index = props.getProperty("index");
		type = props.getProperty("type");
		String payload = "{" + "\"time_stamp\": \"" + log.getTime_stamp() + "\", " + "\"module_name\": \""
				+ log.getModule_name() + "\", " + "\"host\": \"" + log.getHost_name() + "\", " + "\"file\": \""
				+ log.getFile_name() + "\", " + "\"function\": \"" + log.getFunction_name() + "\", " + "\"line\": \""
				+ log.getLine_no() + "\", " + "\"thread\": \"" + log.getThread_no() + "\", " + "\"msg\": \""
				+ log.getMsg() + "\"" + "}";

		StringEntity entity = new StringEntity(payload, ContentType.APPLICATION_JSON);
		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost(url + ":" + port + "/" + index + "/" + type);
		request.setEntity(entity);
		HttpResponse response = httpClient.execute(request);
		System.out.println(response.getStatusLine().getStatusCode());
		return response.getStatusLine().getStatusCode();

	}

}
