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
* @author Chris Jurado, Stephen Granite
* 
*/
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;

public class TimeSeriesStorer {

	private static final String API_METHOD = "/api/put";
	
	public static void storeTimePoint(String urlString, IncomingDataPoint dataPoint){
		int responseCode = 0;
		urlString = urlString + API_METHOD;
		ArrayList<IncomingDataPoint> point = new ArrayList<IncomingDataPoint>();
		point.add(dataPoint);
		try {
			responseCode = TimeSeriesUtility.insertDataPoints(urlString, point);
			if(responseCode > 301 || responseCode == 0){
				throw new OpenTSDBException(responseCode, urlString, dataPoint.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
		}
	}
	
	public static void storeTimePoints(String urlString, List<IncomingDataPoint> dataPoints){
		int responseCode = 0;
		urlString = urlString + API_METHOD;
		
		try {
			responseCode = TimeSeriesUtility.insertDataPoints(urlString, dataPoints);
			if(responseCode > 301 || responseCode == 0){
				throw new OpenTSDBException(responseCode, urlString, dataPoints.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
		}
	}
	
	public static void storeTimePoint(String urlString, String metric, long epochTime, HashMap<String, String> tags){
		urlString = urlString + API_METHOD;
		IncomingDataPoint point = new IncomingDataPoint();
		point.setMetric(metric);
		point.setTags(tags);
		point.setTimestamp(epochTime);
		storeTimePoint(urlString, point);
	}
	
	//The delete methods are not implemented because there is no API mechanism in OpenTSDB 
	//to delete data points at this time.  These methods w ill be implemented once that is available.
	//-CRJ 1 October, 2015
	public static void deleteTimePoint(String urlString, String metric, long epochTime){}
	
	public static void deleteTimeSeries(String urlString, String tsuid){}
	
	protected String getChannelName(int index, String[] channels){
		return channels[index];
	}
}