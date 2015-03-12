package edu.jhu.cvrg.timeseriesstore.opentsdb.store;
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
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Date;

import com.google.gson.Gson;

import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;
import edu.jhu.cvrg.timeseriesstore.util.TimeSeriesUtility;

public abstract class OpenTSDBTimeSeriesStorer {
	
	private final long DEFAULT_TIME = 1420088400L;//1 January, 2015 00:00:00

	public boolean storeTimeSeries(InputStream inputStream, String[] channels, int samples, String urlString, String subjectId){

			urlString = urlString + "/api/put";
			ArrayList<IncomingDataPoint> points = extractTimePoints(inputStream, channels, samples);
			
			if(points == null){
				System.out.println("No data returned. YOU SUCK");
				return false;
			}
			
			for(IncomingDataPoint point : points){
				point.getTags().put("subjectId", subjectId);
			}
			
			return TimeSeriesUtility.insertDataPoints(urlString, points);
	}
	

	
	protected String getChannelName(int index, String[] channels){
		return channels[index];
	}

	protected ArrayList<IncomingDataPoint> extractTimePoints(InputStream inputStream, String[] channels, int samples){

		return extractTimePoints(inputStream, channels, samples, DEFAULT_TIME);
		
	}
	
	protected abstract ArrayList<IncomingDataPoint> extractTimePoints(InputStream inputStream, String[] channels, int samples, long epochTime);
}