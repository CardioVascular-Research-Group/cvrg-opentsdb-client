package edu.jhu.cvrg.timeseriesstore.opentsdb.retrieve;

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
		
	public static JSONArray retrieveTimeSeriesPOST(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags, boolean showTSUIDs){
		      
		urlString = urlString + API_METHOD;
		String result = "";
		
		try{		
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());

			JSONObject mainObject = new JSONObject();
			mainObject.put("start", startEpoch);
			mainObject.put("end", endEpoch);
//			mainObject.put("show_tsuids", showTSUIDs);
			
			JSONArray queryArray = new JSONArray();
			
			JSONObject queryParams = new JSONObject();
			queryParams.put("aggregator", "sum");
			queryParams.put("metric", metric);
//			queryParams.put("showTSUIDs", showTSUIDs);
	
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
	
	public static JSONArray retrieveTimeSeriesGET(String urlString, long startEpoch, long endEpoch, String metric, HashMap<String, String> tags, boolean showTSUIDs){
	
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
		
	
		HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			
		return new JSONArray(handleResponse(httpConnection));
	}
	
	private static String handleResponse(HttpURLConnection httpConnection){
		
		String result = "";
		
		try{
			int httpResult = httpConnection.getResponseCode(); 
			
			if(httpResult == HttpURLConnection.HTTP_OK){
				result = TimeSeriesUtility.readHTTPConnection(httpConnection);
			}else{
				result =  String.valueOf(httpResult);
			}  
			
			httpConnection.disconnect();
			
		} catch (IOException e) {
			e.printStackTrace();
			result = "";
		}
			
		return result;
	}
}