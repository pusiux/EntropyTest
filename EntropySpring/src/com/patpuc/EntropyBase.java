package com.patpuc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntropyBase extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text password = new Text();
	static List<String> pass = new ArrayList<String>();
	static HashMap<String, Double> unsortedMap = new HashMap<String, Double>();
	static ValueComparator bvc = new ValueComparator(unsortedMap);
	static TreeMap<String, Double> sortedMap = new TreeMap<String, Double>(bvc);

	public static void main(String[] args) throws FileNotFoundException, IOException {
		System.out.println("***** START *****");
		PrintWriter writerToFile = new PrintWriter("/home/pusiux/Desktop/passwordsWithEntropy.txt");
		System.out.println("***** START2 *****");

		readFromFileToList();
		System.out.println("***** START3 *****");

		writeEntropyToMap(pass);
		System.out.println("***** START4 *****");

		sortedMap.putAll(unsortedMap);
		System.out.println("***** START5 *****");

		for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
			writerToFile.println(entry.getKey() + " " + entry.getValue());
		}
		writerToFile.close();
		System.out.println("***** SZTOP *****");

	}

	

	public static void readFromFileToList() throws FileNotFoundException, IOException {
		String line;
		int i = 0;
		try (InputStream fis = new FileInputStream("/home/pusiux/Desktop/passwords.txt");
				InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
				BufferedReader br = new BufferedReader(isr);) {
			while ((line = br.readLine()) != null) {
				if (line.length() > 0) {
					i++;
					pass.add(line);
					System.out.println(i);
				}
			}
		}
	}

	public static void writeEntropyToMap(List<String> pass) throws FileNotFoundException {
		List<String> passTmp = new ArrayList<String>();
		Double result = 0.0;
		for (int j = 0; j < pass.size(); j++) {
			System.out.println(j);

			for (int i = 0; i < pass.get(j).length(); i++) {
				passTmp.add(String.valueOf(pass.get(j).charAt(i)));
			}
			result = calculateShannonEntropy(passTmp);
			unsortedMap.put(pass.get(j), result);
			result = 0.0;
		}
	}

	public static Double calculateShannonEntropy(List<String> values) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		// count the occurrences of each value
		for (String sequence : values) {
			if (!map.containsKey(sequence)) {
				map.put(sequence, 0);
			}
			map.put(sequence, map.get(sequence) + 1);
		}

		// calculate the entropy
		Double result = 0.0;
		for (String sequence : map.keySet()) {
			Double frequency = (double) map.get(sequence) / values.size();
			result -= frequency * (Math.log(frequency) / Math.log(2));
		}
		return result;
	}
}

class ValueComparator implements Comparator<String> {
	Map<String, Double> base;

	public ValueComparator(Map<String, Double> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with
	// equals.
	public int compare(String a, String b) {
		if (base.get(b) <= base.get(a)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}

}