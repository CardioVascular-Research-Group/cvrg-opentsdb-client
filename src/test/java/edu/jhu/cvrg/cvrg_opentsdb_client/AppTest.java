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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;
import edu.jhu.cvrg.timeseriesstore.opentsdb.AnnotationManager;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesRetriever;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesStorer;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AppTest extends TestCase
{
	
	final String OPENTSDB_HOST = "10.162.38.31";
	final String OPENTSDB_URL = "http://"+OPENTSDB_HOST+":4242";

    public AppTest(String testName)
    {
        super(testName);
    }
    
    
    
    public void pause(){
    	try {
			Thread.currentThread().sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    @Test
    public void test01InsertSingleDataPoint(){
    	try {
			HashMap<String, String> tags = new HashMap<String,String>();
			tags.put("subjectId", "ncc1701applesauce");
			tags.put("format", "applesauce");
			IncomingDataPoint dataPoint = new IncomingDataPoint("ecg.applesauce.uv", 1420088400L, "16", tags);
			TimeSeriesStorer.storeTimePoint(OPENTSDB_URL, dataPoint);
			assertTrue(testRetrieveSingleDataPoint());
		} catch (OpenTSDBException e) {
			e.printStackTrace();
		}
    }
    
    public boolean testRetrieveSingleDataPoint(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String,String>();
    	int result = 0;
    	tags.put("subjectId", "ncc1701applesauce");
    	tags.put("format", "applesauce");
    	
    	try {
    		JSONArray array = TimeSeriesRetriever.retrieveTimeSeries(OPENTSDB_URL, 1420088400L, 1420088401L, "ecg.applesauce.uv", tags);
			JSONObject object = array.getJSONObject(0);
    		JSONObject data = object.getJSONObject("dps");
			System.out.println(object.toString());
			result = data.getInt("1420088400");
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			return false;
		}
    	return (result == 16);
    }
    
    @Test
    public void test10DeleteSingleDataPoint(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String,String>();
    	int result = 0;
    	tags.put("subjectId", "ncc1701applesauce");
    	//tags.put("format", "applesauce");
    	
    	List<String> metrics = new ArrayList<String>();
    	metrics.add("ecg.applesauce.uv");
    	
    	String out = "";
		try {
			out = TimeSeriesStorer.deleteTimeSeries(OPENTSDB_HOST, 1420088400L, 1420088401L, metrics, tags, "avilard4", "23ram24a@");
		} catch (OpenTSDBException e) {
			e.printStackTrace();
		}
    	
    	assertTrue( out.contains("exit-status: 0"));
    }

    @Test
    public void test03TsuidGet(){
    	pause();
    	String result = ""; 
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	result = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	assertTrue(result != "");
    }
    
    @Test
    public void test04StoreSinglePointAnnotation(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070460L, tsuid, description, "");
    	System.out.println("Single point Annotation result is " + result);
    	assertTrue(testRetrieveSinglePointAnnotation());
    }
    
    public boolean testRetrieveSinglePointAnnotation(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	String description = null;
		try {
			description = result.getString("description");
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		}
    	return ("Test Annotation One".equals(description));
    }
    
    @Test
    public void test06StoreIntervalAnnotation(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createIntervalAnnotation(OPENTSDB_URL, 1420070465L, 1420070467L, tsuid, description, "");
    	System.out.println("Interval Annotation result is " + result);
    	assertTrue(testRetrieveIntervalAnnotation());
    }
    
    public boolean testRetrieveIntervalAnnotation(){
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070465L, tsuid);
    	String description = null;
		try {
			description = result.getString("description");
		} catch (JSONException e) {
			e.printStackTrace();
			return false;
		}
		return ("Test Annotation One".equals(description));
    }
       
    @Test
    public void test08EditAnnotation(){
    	pause();
    	String newDescription = "", newNotes = "";
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	String description = "This should not change.";
    	String notes = "This should change.";
    	AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070490L, tsuid, description, notes);
    	AnnotationManager.editAnnotation(OPENTSDB_URL, 1420070490L, 0L, tsuid, description, "I changed.");
    	JSONObject editedResultJSON = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070490L, tsuid);
    	try {
			newDescription = editedResultJSON.getString("description");
			newNotes = editedResultJSON.getString("notes");
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	assertTrue(description.equals(newDescription) && !notes.equals(newNotes));
    }
    
    @Test
    public void test09Delete(){
    	pause();
    	HashMap<String, String> tags = new HashMap<String, String>();
    	tags.put("subjectId", "ncc1701applesauce");
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, tags, "ecg.applesauce.uv");
    	String code = "";
    	AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070485L, tsuid, "Delete me", "Please");
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	AnnotationManager.deleteAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	try {
			code = result.getString("code");
		} catch (JSONException e) {
			e.printStackTrace();//This will print the 404 error.  This indicates the test is a SUCCESS. -CRJ 2 October 2015
		}	
    	assertTrue(code.equals("404"));
    }   
}
