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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import com.google.gson.Gson;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;

public class TimeSeriesUtility {

	public static boolean insertDataPoints(String urlString, ArrayList<IncomingDataPoint> points){
		Gson gson = new Gson();
		HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
		
		try{
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
						
			String json = gson.toJson(points);

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
	
	public static String handleResponseCode(HttpURLConnection httpConnection){
		String code = "";
		try{
			switch(httpConnection.getResponseCode()){
				case 200:	code = "Response OK";									break;
				case 204:	code = "No data returned. Put OK";						break;
				case 301:	code = "Migrated OK";									break;
				default:	try {
								throw new OpenTSDBException(httpConnection);
							} catch (OpenTSDBException e) {
								e.printStackTrace();
								return "OpenTSDB Error.";
								
							}			
			}
		}
		catch(IOException e){
			e.printStackTrace();
			return "OpenTSDB Error.";
		}
		return code;
	}
}