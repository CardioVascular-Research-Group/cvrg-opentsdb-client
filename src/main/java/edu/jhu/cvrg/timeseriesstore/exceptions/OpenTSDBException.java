package edu.jhu.cvrg.timeseriesstore.exceptions;

import java.io.IOException;
import java.net.HttpURLConnection;

public class OpenTSDBException extends Exception {

	private static final long serialVersionUID = 1L;

	public OpenTSDBException(HttpURLConnection httpConnection) throws IOException{
		super("OpenTSDB Error. Code " + httpConnection.getResponseCode() + ":" + httpConnection.getResponseMessage());
	}
}
