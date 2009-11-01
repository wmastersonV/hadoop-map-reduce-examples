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
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * The mapper class is just responsible for rounding off the Geo Location 
 * and creating a key like this <b> (RoundedGeoLat,RoundedGeoLong) </b>
 * and HTML decode the article title and pass it onto the output collector.
 * all the locations will be rounded off to the closes geo point and be grouped togather
 * we can write advanced distance based keys also (feel free to extend the example) 
 * @author magbeth (Subbu Iyer)
 *
 */

public class GeoLocationMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public static String GEO_RSS_URI = "http://www.georss.org/georss/point";

	private Text geoLocationKey = new Text();
	private Text geoLocationName = new Text();

	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {

		String dataRow = value.toString();

		// since these are tab seperated files lets tokenize on tab
		StringTokenizer dataTokenizer = new StringTokenizer(dataRow, "\t");
		String articleName = dataTokenizer.nextToken();
		String pointType = dataTokenizer.nextToken();
		String geoPoint = dataTokenizer.nextToken();
		// we know that this data row is a GEO RSS type point.
		if (GEO_RSS_URI.equals(pointType)) {
			// now we process the GEO point data.
			StringTokenizer st = new StringTokenizer(geoPoint, " ");
			String strLat = st.nextToken();
			String strLong = st.nextToken();
			double lat = Double.parseDouble(strLat);
			double lang = Double.parseDouble(strLong);
			long roundedLat = Math.round(lat);
			long roundedLong = Math.round(lang);
			String locationKey = "(" + String.valueOf(roundedLat) + ","
					+ String.valueOf(roundedLong) + ")";
			String locationName = URLDecoder.decode(articleName, "UTF-8");
			locationName = locationName.replace("_", " ");
			locationName = locationName + ":(" + lat + "," + lang + ")";
			geoLocationKey.set(locationKey);
			geoLocationName.set(locationName);
			outputCollector.collect(geoLocationKey, geoLocationName);
		}

	}

}
