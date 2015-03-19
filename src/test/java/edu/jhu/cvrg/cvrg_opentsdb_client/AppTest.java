package edu.jhu.cvrg.cvrg_opentsdb_client;

import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.Test;

import edu.jhu.cvrg.timeseriesstore.opentsdb.AnnotationManager;
import edu.jhu.cvrg.timeseriesstore.opentsdb.TimeSeriesRetriever;

public class AppTest 
    extends TestCase
{
	
	final String OPENTSDB_URL = "http://10.162.38.224:4242";

    public AppTest( String testName )
    {
        super( testName );
    }

    @Test
    public void testTsuidGet(){
    	String result = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	assertTrue(result != null);
    }
    
    @Test
    public void testStoreSinglePoint(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070460L, tsuid, description, "");
    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveSinglePoint(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	
    	String description = result.getString("description");
    	assertTrue(description.equals("Test Annotation One"));
    }
    
    @Test
    public void testStoreInterval(){
    	String tsuid = TimeSeriesRetriever.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createIntervalAnnotation(OPENTSDB_URL, 1420070465L, 1420070467L, tsuid, description, "");

    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveInterval(){
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