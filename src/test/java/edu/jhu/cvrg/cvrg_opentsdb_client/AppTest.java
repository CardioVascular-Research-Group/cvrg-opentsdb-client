package edu.jhu.cvrg.cvrg_opentsdb_client;

import junit.framework.TestCase;
import edu.jhu.cvrg.timeseriesstore.util.TimeSeriesUtility;
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
    	System.out.println("Result is " + result);
    	assertTrue(result != null);
    }
}
