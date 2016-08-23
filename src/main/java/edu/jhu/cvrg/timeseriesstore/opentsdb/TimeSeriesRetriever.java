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
* @author Chris Jurado
*/
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;

public class TimeSeriesRetriever{

	private static String API_METHOD = "/api/query/";
	private static String API_SEARCH = "/api/search/lookup";
	
	public static String findTsuid(String urlString, HashMap<String, String> tags, String metric){
		return findTsuid(urlString, tags, metric, TimeSeriesUtility.DEFAULT_START_TIME);
	}
	
	public static String findTsuid(String urlString, HashMap<String, String> tags, String metric, long startTime){
		return TimeSeriesUtility.findTsuid(urlString, tags, metric, startTime);
	} 

	public static JSONObject retrieveTimeSeries(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags, boolean showTSUIDs){
		return retrieveTimeSeriesGET(urlString, startEpoch, endEpoch, metric, tags, showTSUIDs);
	}
	
	public static JSONArray getMetricList(String urlString, String subjectId){
		String result = "";

		urlString = urlString + API_SEARCH;

		StringBuilder builder = new StringBuilder();
		
		builder.append("?m=*{subject_id=");
		builder.append(subjectId);
		builder.append("}&limit=20000");

		try {
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			result = String.valueOf(e.responseCode);
		}
		JSONArray retRet = new JSONArray();
		retRet = TimeSeriesUtility.makeResponseJSONArray(result);
		//System.out.println("metricListGet result: " + retRet.toString());
		return retRet;
		
	}

	public static JSONObject getDownsampledTimeSeries(String urlString, long startEpoch, long endEpoch, String metric, String downsample, HashMap<String, String> tags, boolean showTSUIDs){
		return downsampledTimeseriesGet(urlString, startEpoch, endEpoch, metric, downsample, tags, showTSUIDs);
	}
	
	private static JSONArray retrieveTimeSeriesPOST(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags) throws OpenTSDBException{
		      
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
	
				Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
				while (entries.hasNext()) {
					@SuppressWarnings("rawtypes")
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

			result = TimeSeriesUtility.readHttpResponse(httpConnection);
			
		} catch (IOException e) {
			throw new OpenTSDBException("Unable to connect to server", e);
		} catch (JSONException e) {
			throw new OpenTSDBException("Error on request data", e);
		}
		
		return TimeSeriesUtility.makeResponseJSONArray(result);
	}
	
	private static JSONObject retrieveTimeSeriesGET(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags, boolean showTSUIDs){
	
		urlString = urlString + API_METHOD;
		String result = "";
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("?start=");
		builder.append(startEpoch);
		builder.append("&end=");
		builder.append(endEpoch);
		builder.append("&show_tsuids=");
		builder.append(showTSUIDs);
		builder.append("&m=sum:");
		builder.append(metric);
		
		if(tags != null){
			builder.append("{");
			
			Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
			while (entries.hasNext()) {
				@SuppressWarnings("rawtypes")
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

		try {
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			result = String.valueOf(e.responseCode);
		}
		JSONObject retRet = new JSONObject();
		retRet = TimeSeriesUtility.makeResponseJSONObject(result);
		//System.out.println("timeSeriesGet result: " + retRet.toString());
		return retRet;
	}
	
	private static JSONObject downsampledTimeseriesGet(String urlString, long startEpoch, long endEpoch, String metric, String downsample, HashMap<String, String> tags, boolean showTSUIDs){
	
		urlString = urlString + API_METHOD;
		String result = "";
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("?start=");
		builder.append(startEpoch);
		builder.append("&end=");
		builder.append(endEpoch);
		builder.append("&show_tsuids=");
		builder.append(showTSUIDs);
		builder.append("&m=sum:");
		builder.append(downsample);
		builder.append(":");
		builder.append(metric);
		
		if(tags != null){
			builder.append("{");
			
			Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
			while (entries.hasNext()) {
				@SuppressWarnings("rawtypes")
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

		try {
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			result = String.valueOf(e.responseCode);
		}
		//System.out.println("downsampledTimeSeriesGet result: " + result);
		return TimeSeriesUtility.makeResponseJSONObject(result);
	}
}
