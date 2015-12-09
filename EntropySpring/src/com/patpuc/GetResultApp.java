package com.patpuc;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/getresult")
public class GetResultApp {

	static HashMap<String, Double> unsortedMap = new HashMap<String, Double>();
	static ValueComparatorr bvc = new ValueComparatorr(unsortedMap);
	static TreeMap<String, Double> myMap = new TreeMap<String, Double>(bvc);

	public static TreeMap<String, Double> getMyMap() {
		return myMap;
	}

	public static void setMyMap(TreeMap<String, Double> myMap) {
		GetResultApp.myMap = myMap;
	}

	@RequestMapping(method = RequestMethod.GET)
	public String printHello(ModelMap model) throws Exception {
        myMap.clear();
		model.addAttribute("message", "Results sorted by entropy: ");
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		FileStatus[] fss = fs.listStatus(new Path("hdfs://localhost:9000/user/pusiux/output/part-00000"));
		
			for (FileStatus status : fss) {
				Path path = status.getPath();
				SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(),
						SequenceFile.Reader.file(path));
				Text key = new Text();
				DoubleWritable value = new DoubleWritable();
				while (reader.next(key, value)) {
					if ((key.toString()).length() > 0) {

						unsortedMap.put(key.toString(), value.get());
					}
				}
				reader.close();
			}
		
		
		myMap.putAll(unsortedMap);
		model.addAttribute("myMap", myMap);
		return "getresult";
	}
}

class ValueComparatorr implements Comparator<String> {
	Map<String, Double> base;

	public ValueComparatorr(Map<String, Double> base) {
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