package edu.jhu.cvrg.cvrg_opentsdb_client;
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
*/
import java.util.HashMap;

import junit.framework.TestCase;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;
import edu.jhu.cvrg.timeseriesstore.opentsdb.AnnotationManager;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesRetriever;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesStorer;

public class AppTest extends TestCase
{
	final String OPENTSDB_URL = "http://10.162.38.224:4242";

    public AppTest(String testName)
    {
        super(testName);
    }
    
    @Test
    public void testInsertSingleDataPoint(){
    	HashMap<String, String> tags = new HashMap<String,String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	tags.put("format", "applesauce");
    	IncomingDataPoint dataPoint = new IncomingDataPoint("ecg.applesauce.uv", 1420088400L, "16", tags);
    	TimeSeriesStorer.storeTimePoint(OPENTSDB_URL, dataPoint);
    	assertTrue(true);
    }
    
    @Test
    public void testRetrieveSingleDataPoint(){
    	HashMap<String, String> tags = new HashMap<String,String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	tags.put("format", "applesauce");
    	JSONObject object = TimeSeriesRetriever.retrieveTimeSeries(OPENTSDB_URL, 1420088400L, 1420088401L, "ecg.applesauce.uv", tags);
    	JSONObject data = object.getJSONObject("dps");
    	System.out.println(object.toString());
    	assertTrue(data.getInt("1420088400") == 16);
    }

    @Test
    public void testTsuidGet(){
    	String result = "";  	
    	result = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	assertTrue(result != "");
    }
    
    @Test
    public void testStoreSinglePointAnnotation(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070460L, tsuid, description, "");
    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveSinglePointAnnotation(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	String description = result.getString("description");
    	assertTrue(description.equals("Test Annotation One"));
    }
    
    @Test
    public void testStoreIntervalAnnotation(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createIntervalAnnotation(OPENTSDB_URL, 1420070465L, 1420070467L, tsuid, description, "");
    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveIntervalAnnotation(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070465L, tsuid);
    	String description = result.getString("description");
    	assertTrue(description.equals("Test Annotation One"));
    }
       
    @Test
    public void testEditAnnotation(){
    	String newDescription, newNotes = "";
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "This should not change.";
    	String notes = "This should change.";
    	AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070490L, tsuid, description, notes);
    	AnnotationManager.editAnnotation(OPENTSDB_URL, 1420070490L, 0L, tsuid, description, "I changed.");
    	JSONObject editedResultJSON = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070470L, tsuid);
    	newDescription = editedResultJSON.getString("description");
    	newNotes = editedResultJSON.getString("notes");
    	assertTrue(description.equals(newDescription) && !notes.equals(newNotes));
    }
    
    @Test
    public void testDelete(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String code = "";
    	AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070485L, tsuid, "Delete me", "Please");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	AnnotationManager.deleteAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	code = result.getString("code");	
    	assertTrue(code.equals("404"));
    }   
}