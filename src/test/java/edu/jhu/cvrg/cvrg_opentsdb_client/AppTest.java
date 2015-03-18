package edu.jhu.cvrg.cvrg_opentsdb_client;

import junit.framework.TestCase;
import edu.jhu.cvrg.timeseriesstore.opentsdb.annotations.AnnotationManager;
import edu.jhu.cvrg.timeseriesstore.util.TimeSeriesUtility;

import org.json.JSONObject;
import org.junit.Test;

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
    	String result = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	assertTrue(result != null);
    }
    
    @Test
    public void testStoreSinglePoint(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070460L, tsuid, description, "");
    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveSinglePoint(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	
    	String description = result.getString("description");
    	assertTrue(description.equals("Test Annotation One"));
    }
    
    @Test
    public void testStoreInterval(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String description = "Test Annotation One";
    	String result = AnnotationManager.createIntervalAnnotation(OPENTSDB_URL, 1420070465L, 1420070467L, tsuid, description, "");

    	assertTrue(result != "");
    }
    
    @Test
    public void testRetrieveInterval(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070465L, tsuid);
    	
    	String description = result.getString("description");
    	assertTrue(description.equals("Test Annotation One"));
    }
    
    @Test
    public void testEditAnnotation(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String initial = "initial";
    	String after = "after";

    	//TODO:  Don't change things that aren't meant to change
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	
    	initial = result.toString();
    	
    	String editResult = AnnotationManager.editAnnotation(OPENTSDB_URL, 1420070460L, 0L, tsuid, "Edited Annotation", "Some notes");
    	
    	JSONObject editedResultJSON = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070460L, tsuid);
    	
    	after = editedResultJSON.toString();
    	
    	assertTrue(!initial.equals(after));
    }
    
    @Test
    public void testPartialEditAnnotation(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");
    	String initial = "initial";
    	String after = "after";
    	
    	String description = "This should not change.";
    	String notes = "This should change.";
    	
    	String result = AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070470L, tsuid, description, notes);
    	
    	String editResult = AnnotationManager.editAnnotation(OPENTSDB_URL, 1420070470L, 0L, tsuid, "", "This changed.");
    	
    	JSONObject editedResultJSON = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070470L, tsuid);
    	
    	after = editedResultJSON.toString();
    	
    	description = editedResultJSON.getString("description");
    	notes = editedResultJSON.getString("notes");
    	
    	assertTrue(description.equals("This should not change.") && notes.equals("This changed."));
    }
    
    @Test
    public void testDelete(){
    	String tsuid = TimeSeriesUtility.findTsuid(OPENTSDB_URL, "ncc1701E", "ecg.I.uv");

    	AnnotationManager.createSinglePointAnnotation(OPENTSDB_URL, 1420070485L, tsuid, "Delete me", "Please");
    	    	
    	JSONObject result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	
    	AnnotationManager.deleteAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	
    	result = AnnotationManager.queryAnnotation(OPENTSDB_URL, 1420070485L, tsuid);
    	
    	assertTrue(result == null);
    }
    
    
}
