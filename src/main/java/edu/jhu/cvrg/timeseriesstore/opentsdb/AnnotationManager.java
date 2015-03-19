package edu.jhu.cvrg.timeseriesstore.opentsdb;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;

import org.json.JSONObject;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;

public class AnnotationManager {
	
	public static final String API_METHOD = "/api/annotation";
	
	public static String createSinglePointAnnotation(String urlString, long startEpoch, String tsuid, String description, String notes){
		return createIntervalAnnotation(urlString, startEpoch, 0L, tsuid, description, notes);
	}
	
	public static String createIntervalAnnotation(String urlString, long startEpoch, long endEpoch, String tsuid, String description, String notes){
				
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
			requestObject.put("notes", notes);
			
			wr.write(requestObject.toString());
			wr.close();
			
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			result = String.valueOf(e.responseCode);
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
	
		try {
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionGET(urlString + builder.toString());
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
		} catch (OpenTSDBException e) {
			result = String.valueOf(e.responseCode);
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		return TimeSeriesUtility.makeResponseJSONObject(result);
	}
	
	public static String editAnnotation(String urlString, long startEpoch, long endEpoch, String tsuid, String description, String notes){
		
		JSONObject annotation = queryAnnotation(urlString, startEpoch, tsuid);

		String descriptionOld = annotation.getString("description");
		String notesOld = annotation.getString("notes");

		description = (description.equals("")) ? descriptionOld : description;
		notes = (notes.equals("")) ? notesOld : notes;
		
		return createIntervalAnnotation(urlString, startEpoch, endEpoch, tsuid, description, notes);
	}
	
	public static String deleteAnnotation(String urlString, long startEpoch, String tsuid){
		
		urlString = urlString + API_METHOD;
		String result = "";
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("?start_time=");
		builder.append(startEpoch);
		builder.append("&tsuid=");
		builder.append(tsuid);
		
		try{		
			HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionDELETE(urlString + builder.toString());
			result = TimeSeriesUtility.readHttpResponse(httpConnection);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			result = String.valueOf(e.responseCode);
		}
	
		return result;
	}
}