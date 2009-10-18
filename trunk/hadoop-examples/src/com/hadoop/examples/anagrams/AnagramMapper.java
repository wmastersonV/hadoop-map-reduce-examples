package com.hadoop.examples.anagrams;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/**
 * The Anagram mapper class gets a word as a line from the HDFS input and sorts the
 * letters in the word and writes its back to the output collector as 
 * Key : sorted word (letters in the word sorted)
 * Value: the word itself as the value.
 * When the reducer runs then we can group anagrams togather based on the sorted key.
 * 
 * @author subbu iyer
 *
 */
public class AnagramMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text sortedText = new Text();
	private Text orginalText = new Text();

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {

		String word = value.toString();
		char[] wordChars = word.toCharArray();
		Arrays.sort(wordChars);
		String sortedWord = new String(wordChars);
		sortedText.set(sortedWord);
		orginalText.set(word);
		outputCollector.collect(sortedText, orginalText);
	}

}
