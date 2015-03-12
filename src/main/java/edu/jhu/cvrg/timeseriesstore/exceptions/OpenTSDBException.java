package edu.jhu.cvrg.timeseriesstore.exceptions;
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
* @author Chris Jurado
* 
*/
import java.io.IOException;
import java.net.HttpURLConnection;

public class OpenTSDBException extends Exception {

	private static final long serialVersionUID = 1L;

	public OpenTSDBException(HttpURLConnection httpConnection) throws IOException{
		super("OpenTSDB Error. Code " + httpConnection.getResponseCode() + ":" + httpConnection.getResponseMessage());
	}
}
