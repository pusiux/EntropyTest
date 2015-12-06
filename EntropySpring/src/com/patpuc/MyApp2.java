package com.patpuc;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/getresult")
public class MyApp2{
	Map<String, Integer> myMap = new HashMap<String, Integer>();
   
	
public Map<String, Integer> getMyMap() {
		return myMap;
	}


	public void setMyMap(Map<String, Integer> myMap) {
		this.myMap = myMap;
	}


@RequestMapping(method = RequestMethod.GET)
   public String printHello(ModelMap model) throws Exception {
	 
      model.addAttribute("message", "Hello Spring MVC Framework!");
      Configuration conf = new Configuration ();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/home/pusiux/hadoop-2.6.1/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		System.out.println("xx");
		
		FileStatus[] fss = fs.listStatus(new Path("hdfs://localhost:9000/user/pusiux/output/part-00000"));
		
      for (FileStatus status : fss) {
          Path path = status.getPath();
          SequenceFile.Reader reader = 
                  new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(path));
          Text key = new Text();
          IntWritable value = new IntWritable();
          while (reader.next(key, value)) {
              System.out.println(key.toString() + " | " + value.get());
              myMap.put(key.toString(), value.get());
          }
          reader.close();
      }
      model.addAttribute("myMap", myMap);
     
      return "getresult";
   }

}