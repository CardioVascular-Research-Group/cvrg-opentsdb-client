package edu.jhu.cvrg.timeseriesstore.util;
/*
Copyright 2015 Johns Hopkins University Institute for Computational Medicine

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/**
* @author Stephen Granite, Chris Jurado
* 
*/
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
//import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;
import edu.jhu.cvrg.timeseriesstore.opentsdb.retrieve.OpenTSDBTimeSeriesRetriever;

public class TimeSeriesUtility {
	
	public static long DEFAULT_START_TIME = 1420088400L;//1 January, 2015 00:00:00
	
	public static String findTsuid(String urlString, String subjectId, String metric){
		
		
		return findTsuid(urlString, subjectId, metric, DEFAULT_START_TIME);

	}
	
	public static String findTsuid(String urlString, String subjectId, String metric, long startTime){
		
		String tsuid = "";
		long endTime = startTime + 1;
		HashMap<String, String> tags = new HashMap<String, String>();
		JSONArray results = null;
		JSONObject dataObject = null;
		
		tags.put("subjectId", subjectId);
		
		results = OpenTSDBTimeSeriesRetriever.retrieveTimeSeriesGET(urlString, startTime, endTime, metric, tags, true);
		dataObject = results.getJSONObject(0);

		JSONArray tsuids = dataObject.getJSONArray("tsuids");
		tsuid = tsuids.getString(0);
		
		return tsuid;	
	}
	
	public static boolean insertDataPoints(String urlString, ArrayList<IncomingDataPoint> points){
		Gson gson = new Gson();
		HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
		
		try{
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
						
			String json = gson.toJson(points);
			System.out.println(json.toString());
			wr.write(json);
			wr.flush();
			wr.close();
		
			int HttpResult = httpConnection.getResponseCode(); 
		
			handleResponseCode(httpConnection);
			
			if(HttpResult == HttpURLConnection.HTTP_OK){
				readHTTPConnection(httpConnection);
			}else{
				System.out.println(httpConnection.getResponseMessage());  
			}  
			httpConnection.disconnect();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static HttpURLConnection openHTTPConnectionGET(String urlString){
		
		return openHTTPConnection(urlString, false);
	}
	
	public static HttpURLConnection openHTTPConnectionPOST(String urlString){
	
		return openHTTPConnection(urlString, true);
	}
	
	private static HttpURLConnection openHTTPConnection(String urlString, boolean isPost){
		URL url = null;
		HttpURLConnection conn = null;
		
		try {
			url = new URL(urlString);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-type", "application/json");
			if(isPost){
				conn.setRequestMethod("POST");
				conn.setDoOutput(true);
				conn.setDoInput(true);
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static String readHTTPConnection(HttpURLConnection conn){
		StringBuilder sb = new StringBuilder();  
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));
			String line = null;  
			while ((line = br.readLine()) != null) {  
				sb.append(line + "\n");  
			}  
			br.close(); 
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public static File createTempFile(InputStream inputStream, String version){
		
		String filePath = "/opt/liferay/temp/" + version + ".tempfile";
		File file = new File(filePath);
		try {
			FileUtils.copyInputStreamToFile(inputStream, file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return file;
	}
	
	public static void handleResponseCode(HttpURLConnection httpConnection){
		try{
			switch(httpConnection.getResponseCode()){
				case 200:	System.out.println("Response OK");									break;
				case 204:	System.out.println("No data returned. Put OK");						break;
				case 301:	System.out.println("Migrated OK");									break;
				default:	try {
								throw new OpenTSDBException(httpConnection);
							} catch (OpenTSDBException e) {
//								log.error(e.getStackTrace());
							}			
				break;
			}
		}
		catch(IOException e){
//			log.error(e.getStackTrace());
		}
	}



}
