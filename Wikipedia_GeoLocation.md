## Introduction ##
Db-Pedia has a nice data dump of geo locations of wikipedia articles the objective is to group articles to-gather by their geo location and dump the output.

## Data Set ##
[Geo Location Data From DB-Pedia in CSV format](http://downloads.dbpedia.org/3.3/en/pagelinks_en.csv.bz2)
Geographic coordinates extracted from Wikipedia.


## Details: Mapper ##
The Mapper here is responsible for just extracting the right geo location from the given set (please refer data) and round the latitude and the longitude
```
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
                        geoLocationKey.set(locationKey);
                        geoLocationName.set(locationName);
                        outputCollector.collect(geoLocationKey, geoLocationName);
                }

        }
```

## Details: Reducer ##
The reducer just groups the data and writes the output with comma separation
```

public void reduce(Text anagramKey, Iterator<Text> anagramValues,
                        OutputCollector<Text, Text> results, Reporter reporter)
                        throws IOException {
                // in this case the reducer just creates a list so that the data can
                // used later
                String outputText = "";
                while (anagramValues.hasNext()) {
                        Text locationName = anagramValues.next();
                        outputText = outputText + locationName.toString() + " ,";
                }
                outputKey.set(anagramKey.toString());
                outputValue.set(outputText);
                results.collect(outputKey, outputValue);
        }

```