package edu.jhu.cvrg.timeseriesstore.opentsdb;
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
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import edu.jhu.cvrg.timeseriesstore.enums.HttpVerbs;
import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesRetriever;

public class TimeSeriesUtility {
	
	protected static long DEFAULT_START_TIME = 1420088400L;//1 January, 2015 00:00:00
	
	protected static String findTsuid(String urlString, String subjectId, String metric){
		return findTsuid(urlString, subjectId, metric, DEFAULT_START_TIME);
	}
	
	protected static String findTsuid(String urlString, String subjectId, String metric, long startTime){
		
		String tsuid = "";
		long endTime = startTime + 1;
		HashMap<String, String> tags = new HashMap<String, String>();
		JSONArray results = null;
		JSONObject dataObject = null;
		
		tags.put("subjectId", subjectId);
		
		results = TimeSeriesRetriever.retrieveTimeSeriesGET(urlString, startTime, endTime, metric, tags, true);
		dataObject = results.getJSONObject(0);

		JSONArray tsuids = dataObject.getJSONArray("tsuids");
		
		tsuid = tsuids.getString(0);
		
		return tsuid;	
	}
	
	protected static int insertDataPoints(String urlString, ArrayList<IncomingDataPoint> points) throws IOException{
		int code = 0;
		Gson gson = new Gson();

		HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
		OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
						
		String json = gson.toJson(points);

		wr.write(json);
		wr.flush();
		wr.close();
			
		code = httpConnection.getResponseCode();
		
		httpConnection.disconnect();

		return code;
	}
	
	protected static HttpURLConnection openHTTPConnectionGET(String urlString) throws MalformedURLException, IOException{
		return openHTTPConnection(urlString, HttpVerbs.GET);
	}
	
	protected static HttpURLConnection openHTTPConnectionPOST(String urlString) throws MalformedURLException, IOException{	
		return openHTTPConnection(urlString, HttpVerbs.POST);
	}
	
	protected static HttpURLConnection openHTTPConnectionPUT(String urlString) throws MalformedURLException, IOException{	
		return openHTTPConnection(urlString, HttpVerbs.PUT);
	}
	
	protected static HttpURLConnection openHTTPConnectionDELETE(String urlString) throws MalformedURLException, IOException{
		return openHTTPConnection(urlString, HttpVerbs.DELETE);
	}
	
	private static HttpURLConnection openHTTPConnection(String urlString, HttpVerbs verb) throws MalformedURLException, IOException{
		URL url = null;
		HttpURLConnection conn = null;
		
		url = new URL(urlString);
		conn = (HttpURLConnection) url.openConnection();
			
		switch(verb){
			case POST:		conn.setRequestMethod("POST");
							conn.setDoOutput(true);
							conn.setDoInput(true);				break;
			case PUT:		conn.setRequestMethod("PUT");		
							conn.setDoOutput(true);				break;
			case DELETE:	conn.setRequestMethod("DELETE");	break;
			default:		conn.setRequestMethod("GET");		break;
		}
					
		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Content-type", "application/json");

		return conn;
	}
	
	protected static String readHTTPConnection(HttpURLConnection conn) throws UnsupportedEncodingException, IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br;

		br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line + "\n");
		}
		br.close();

		return sb.toString();
	}
	
	protected static String readHttpResponse(HttpURLConnection httpConnection) throws OpenTSDBException, IOException{
		String result = "";
		int responseCode = httpConnection.getResponseCode();
	
		if(responseCode == 200){
			result = readHTTPConnection(httpConnection);
		}else if(responseCode == 204){
			result = String.valueOf(responseCode);
		}else{
			throw new OpenTSDBException(responseCode, httpConnection.getURL().toString(), ""); 
		} 
		httpConnection.disconnect();
		
		return result;
	}
	
	protected static JSONObject makeResponseJSONObject(String data){
		
		JSONObject product = null;
		try{
			product = new JSONObject(data);
		} catch (JSONException e){
			product = new JSONObject();
			product.put("code", data);
		}

		return product;
	}
	
	protected static JSONArray makeResponseJSONArray(String data){
		
		JSONArray array = null;
		try{
			array = new JSONArray(data);
		} catch (JSONException e){
			array = new JSONArray();
			array.put(makeResponseJSONObject(data));
		}

		return array;
	}
}