/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hadoop.examples.geolocation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * the reducer is responsible for collecting the mapped output  and just appending the values 
 * with comma and generating the output.
 * @author subbu.nagarajan
 *
 */

public class GeoLocationReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	
	public void reduce(Text geoLocationKey, Iterator<Text> geoLocationValues,
			OutputCollector<Text, Text> results, Reporter reporter)
			throws IOException {
		// in this case the reducer just creates a list so that the data can
		// used later
		String outputText = "";
		while (geoLocationValues.hasNext()) {
			Text locationName = geoLocationValues.next();
			outputText = outputText + locationName.toString() + " ,";
		}
		outputKey.set(geoLocationKey.toString());
		outputValue.set(outputText);
		results.collect(outputKey, outputValue);
	}

}
