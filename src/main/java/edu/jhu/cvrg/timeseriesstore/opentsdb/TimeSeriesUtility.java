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
* @author Stephen Granite, Chris Jurado
* 
*/
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

import edu.jhu.cvrg.timeseriesstore.enums.HttpVerbs;
import edu.jhu.cvrg.timeseriesstore.exceptions.OpenTSDBException;
import edu.jhu.cvrg.timeseriesstore.model.IncomingDataPoint;

public class TimeSeriesUtility {
	
	protected static long DEFAULT_START_TIME = 1420088400L;//1 January, 2015 00:00:00
	
	protected static String findTsuid(String urlString, HashMap<String, String> tags, String metric){
		return findTsuid(urlString, tags, metric, DEFAULT_START_TIME);
	}
	
	protected static String findTsuid(String urlString, HashMap<String, String> tags, String metric, long startTime){

		long endTime = startTime + 1;
		JSONObject results = null;	
		results = TimeSeriesRetriever.retrieveTimeSeries(urlString, startTime, endTime, metric, tags, true);
		String ret = null;
		try {
			JSONArray tsuidsArray = results.getJSONArray("tsuids");
			ret = tsuidsArray.getString(0);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	
		return ret;
	}
	
	protected static int insertDataPoints(String urlString, List<IncomingDataPoint> points) throws IOException{
		int code = 0;
		Gson gson = new Gson();

		HttpURLConnection httpConnection = TimeSeriesUtility.openHTTPConnectionPOST(urlString);
		OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());
						
		String json = gson.toJson(points);

		wr.write(json);
		wr.flush();
		wr.close();
			
		code = httpConnection.getResponseCode();
		
		httpConnection.disconnect();

		return code;
	}
	
	protected static HttpURLConnection openHTTPConnectionGET(String urlString) throws MalformedURLException, IOException{
		return openHTTPConnection(urlString, HttpVerbs.GET);
	}
	
	protected static HttpURLConnection openHTTPConnectionPOST(String urlString) throws MalformedURLException, IOException{	
		return openHTTPConnection(urlString, HttpVerbs.POST);
	}
	
	protected static HttpURLConnection openHTTPConnectionPUT(String urlString) throws MalformedURLException, IOException{	
		return openHTTPConnection(urlString, HttpVerbs.PUT);
	}
	
	protected static HttpURLConnection openHTTPConnectionDELETE(String urlString) throws MalformedURLException, IOException{
		return openHTTPConnection(urlString, HttpVerbs.DELETE);
	}
	
	private static HttpURLConnection openHTTPConnection(String urlString, HttpVerbs verb) throws MalformedURLException, IOException{
		URL url = null;
		HttpURLConnection conn = null;
		
		url = new URL(urlString);
		conn = (HttpURLConnection) url.openConnection();
			
		switch(verb){
			case POST:		conn.setRequestMethod("POST");
							conn.setDoOutput(true);
							conn.setDoInput(true);				break;
			case PUT:		conn.setRequestMethod("PUT");		
							conn.setDoOutput(true);				break;
			case DELETE:	conn.setRequestMethod("DELETE");	break;
			default:		conn.setRequestMethod("GET");		break;
		}
					
		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Content-type", "application/json");

		return conn;
	}
	
	protected static String readHTTPConnection(HttpURLConnection conn) throws UnsupportedEncodingException, IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br;

		br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line + "\n");
		}
		br.close();

		return sb.toString();
	}
	
	protected static String readHttpResponse(HttpURLConnection httpConnection) throws OpenTSDBException, IOException{
		String result = "";
		int responseCode = httpConnection.getResponseCode();
	
		if(responseCode == 200){
			result = readHTTPConnection(httpConnection);
		}else if(responseCode == 204){
			result = String.valueOf(responseCode);
		}else{
			throw new OpenTSDBException(responseCode, httpConnection.getURL().toString(), ""); 
		} 
		httpConnection.disconnect();
		
		return result;
	}
	
	protected static JSONObject makeResponseJSONObject(String data){
	
		JSONArray product = null;
		JSONObject ret = null;
		try{
			product = new JSONArray(data);
			ret = (JSONObject) product.get(0);
		} catch (JSONException e){
			e.printStackTrace();
		}

		return ret;
	}
	
	protected static JSONArray makeResponseJSONArray(String data){
		
		JSONArray array = null;
		try{
			array = new JSONArray(data);
		} catch (JSONException e){
			array = new JSONArray();
			array.put(makeResponseJSONObject(data));
		}

		return array;
	}
	
	protected static String executeSSHRemoteCommand(String host, String command, String user, String password) throws OpenTSDBException{
		JSch jsch=new JSch();  
		
		StringBuilder sb = new StringBuilder();
		
		try {
			Session session=jsch.getSession(user, host, 22);
			
			//username and password will be given via UserInfo interface.
			UserInfo ui= new MyUserInfo(password);
			session.setUserInfo(ui);
			session.connect();
			
			Channel channel=session.openChannel("exec");
		    
			((ChannelExec)channel).setCommand(command);
			
			channel.setInputStream(null);
			((ChannelExec)channel).setErrStream(System.err);
		    InputStream in=channel.getInputStream();

		    channel.connect();

		    byte[] tmp=new byte[1024];
		    while(true){
		    	while(in.available()>0){
		          int i=in.read(tmp, 0, 1024);
		          if(i<0)break;
		          sb.append(new String(tmp, 0, i));
		        }
		    	
		        if(channel.isClosed()){
		          if(in.available()>0) continue; 
		          sb.append("exit-status: "+channel.getExitStatus());
		          break;
		        }
		        try{Thread.sleep(1000);}catch(Exception ee){}
		    }
		    channel.disconnect();
		    session.disconnect();
			
			
		} catch (JSchException e) {
			throw new OpenTSDBException("Unable to connect to OpenTSDB host.", e);
		} catch (IOException e) {
			throw new OpenTSDBException("Unable to connect to OpenTSDB host.", e);
		}
		
		return sb.toString();
	}
	
	protected static class MyUserInfo implements UserInfo{
		
		private String passwd;
		
		public MyUserInfo(String password) {
			passwd = password;
		}
		
	    public String getPassword(){ return passwd; }
	    public boolean promptYesNo(String str){ return true; }
	    public String getPassphrase(){ return null; }
	    public boolean promptPassphrase(String message){ return true; }
	    public boolean promptPassword(String message){ return true; }
	    
	    public void showMessage(String message){
	      System.out.println(message);
	    }
	  }
}
