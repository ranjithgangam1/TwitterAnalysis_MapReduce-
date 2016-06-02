package Tweets;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//import org.json.JSONObject;
//import org.json.JSONObject;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

        
public class Tweets_MapReduce 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	private Text word = new Text();
    private Text outval = new Text();    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	
		JSONParser jparser = new JSONParser();
		try
		{
		Object obj = jparser.parse(value.toString());
		
		JSONObject jObject = (JSONObject)obj;
		JSONObject jObject_user = new JSONObject();
		jObject_user = (JSONObject)jObject.get("user");
		
		String name = jObject_user.get("screen_name").toString();
		//System.out.println(name);
		if(name.equals("xxbre14"))
		{
			System.out.println("xxbre14");
			word.set("xxbre14");
			String time = jObject.get("created_at").toString();
			System.out.println(time);
			context.write(word,new Text(time));
	    }

	  }
		catch(Exception e)
		{
			System.out.print("exception occured");
		} 
		
    } 
    
    }
    public static class Reduce extends Reducer<Text, Text, Text, FloatWritable>
    {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		HashMap<String,Integer> Hourcount = new HashMap<String,Integer>();
		String time = "";
		String timearr[];
		String hour[];
		Iterator<Entry<String,Integer>> itr = Hourcount.entrySet().iterator();
		int noofElems =0;
		for(Text val:values)
		{
		time = val.toString();
		timearr =time.split(" ");
		String dates= timearr[1]+timearr[2];
		ArrayList<String> alldates = new ArrayList<String>();
		if(alldates.contains(dates))
		{
			
		}else{
			alldates.add(dates);
		}
		noofElems=alldates.size();	
		
		hour=timearr[3].split(":");
		if(Hourcount.containsKey(hour[0]))
		{
			Hourcount.put(hour[0],Hourcount.get(hour[0])+1);
		}
		else
		{
			Hourcount.put(hour[0], 1);
		}
		//context.write(key, values);
	    }
		Set set = Hourcount.keySet();
		String key1;
        int value;
       float prop;
	    Iterator<String> iter = set.iterator();
	    while(iter.hasNext())
	    {
	    	key1 =iter.next().toString();
	    	value= Hourcount.get(key1);
	    	prop = (float)value/noofElems;
	    	
	    	context.write(new Text(key1), new FloatWritable(prop));
		    
	    }
		
    }
      
    }
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PrezOnoTime");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJarByClass(Tweets_MapReduce.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
/*        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	*/
	/*added */
	String[] otherArgs = new GenericOptionsParser(conf, args)
	.getRemainingArgs();
	
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	/*added ended*/
	
        
	job.waitForCompletion(true);
    }
        
}
