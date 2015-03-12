package edu.jhu.cvrg.timeseriesstore.opentsdb.retrieve;
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
* @author Chris Jurado
* 
*/
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.jhu.cvrg.timeseriesstore.util.TimeSeriesUtility;

public class OpenTSDBTimeSeriesRetriever{

	private static String API_METHOD = "/api/query/";
		
	public static JSONArray retrieveTimeSeriesPOST(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags){
		      
		urlString = urlString + API_METHOD;
		String result = "";
		
		try{		
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());

			JSONObject mainObject = new JSONObject();
			mainObject.put("start", startEpoch);
			mainObject.put("end", endEpoch);
			
			JSONArray queryArray = new JSONArray();
			
			JSONObject queryParams = new JSONObject();
			queryParams.put("aggregator", "sum");
			queryParams.put("metric", metric);
	
			queryArray.put(queryParams);

			if(tags != null){
				JSONObject queryTags = new JSONObject();
	
				Iterator entries = tags.entrySet().iterator();
				while (entries.hasNext()) {
					Map.Entry entry = (Map.Entry) entries.next();
				    queryTags.put((String)entry.getKey(), (String)entry.getValue());
				}
	
				queryParams.put("tags", queryTags);	
			}
			
			mainObject.put("queries", queryArray);
			String queryString = mainObject.toString();
	
			wr.write(queryString);
			wr.flush();
			wr.close();

			int HttpResult = httpConnection.getResponseCode(); 
		
			if(HttpResult == HttpURLConnection.HTTP_OK){
				result = TimeSeriesUtility.readHTTPConnection(httpConnection);
			}else{
				System.out.println(httpConnection.getResponseMessage() + httpConnection.getResponseCode());  
			}  
			
			httpConnection.disconnect();
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return new JSONArray(result);
	}
	
	public static JSONArray retrieveTimeSeriesGET(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags){
	
		urlString = urlString + API_METHOD;
		String result = "";
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("?start=");
		builder.append(startEpoch);
		builder.append("&end=");
		builder.append(endEpoch);
		builder.append("&m=sum:");
		builder.append(metric);
		
		if(tags != null){
			builder.append("{");
			
			Iterator entries = tags.entrySet().iterator();
			while (entries.hasNext()) {
				Map.Entry entry = (Map.Entry) entries.next();
				builder.append((String)entry.getKey());
				builder.append("=");
				builder.append((String)entry.getValue());
				if(entries.hasNext()){
					builder.append(",");
				}
			}
			builder.append("}");
		}
		
		try{		
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			
			int HttpResult = httpConnection.getResponseCode(); 
		
			if(HttpResult == HttpURLConnection.HTTP_OK){
				result = TimeSeriesUtility.readHTTPConnection(httpConnection);
			}else{
				System.out.println(httpConnection.getResponseMessage() + httpConnection.getResponseCode());  
			}  
			
			httpConnection.disconnect();
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return new JSONArray(result);
	}
}