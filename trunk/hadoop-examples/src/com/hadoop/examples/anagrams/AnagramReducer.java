package com.hadoop.examples.anagrams;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * The Anagram reducer class groups the values of the sorted keys that came in and 
 * checks to see if the values iterator contains more than one word. if the values 
 * contain more than one word we have spotted a anagram.
 * @author subbu
 *
 */

public class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	public void reduce(Text anagramKey, Iterator<Text> anagramValues,
			OutputCollector<Text, Text> results, Reporter reporter) throws IOException {
		String output = "";
		while(anagramValues.hasNext())
		{
			Text anagam = anagramValues.next();
			output = output + anagam.toString() + "~";
		}
		StringTokenizer outputTokenizer = new StringTokenizer(output,"~");
		if(outputTokenizer.countTokens()>=2)
		{
			output = output.replace("~", ",");
			outputKey.set(anagramKey.toString());
			outputValue.set(output);
			results.collect(outputKey, outputValue);
		}
	}

}
