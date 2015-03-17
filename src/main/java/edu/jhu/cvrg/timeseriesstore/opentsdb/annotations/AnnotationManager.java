package edu.jhu.cvrg.timeseriesstore.opentsdb.annotations;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;

import org.json.JSONObject;

import edu.jhu.cvrg.timeseriesstore.util.TimeSeriesUtility;

public class AnnotationManager {
	
	public static final String API_METHOD = "/api/annotation";
	
	public static String createSinglePointAnnotation(String urlString, long startEpoch, String tsuid, String description){
		return createIntervalAnnotation(urlString, startEpoch, 0L, tsuid, description);
	}
	
	public static String createIntervalAnnotation(String urlString, long startEpoch, long endEpoch, String tsuid, String description){
				
		urlString = urlString + API_METHOD;
		String result = "";
		
		try{		
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
			
			JSONObject requestObject = new JSONObject();
			requestObject.put("startTime", startEpoch);
			requestObject.put("endTime", endEpoch);
			requestObject.put("tsuid", tsuid);
			requestObject.put("description", description);	
			
			wr.write(requestObject.toString());
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

		return result;

	}
	
	public static JSONObject queryAnnotation(String urlString, long startEpoch, String tsuid){
		
		urlString = urlString + API_METHOD;
		String result = "";
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("?start_time=");
		builder.append(startEpoch);
		builder.append("&tsuid=");
		builder.append(tsuid);
		
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
	
		return new JSONObject(result);
	}
	
	public static Object editAnnotation(long startTime, String tsuid){
		
		return null;
	}
	
	public static Object deleteAnnotation(long startTime, String tsuid){
		
		return null;
	}
}
