import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class ImagePDF {
	public static class PDFMapper extends Mapper<Text, KUPDF, Text, KUPDF> {
        private static final Log log = LogFactory.getLog(ImagePDF.class);
		String dirName = null;
		String fileName = null;
	@Override
	 public void map(Text key, KUPDF value, Context context) throws IOException, InterruptedException {
	    	try{
	    		for(int i=0; i<value.bufferList.size();i++){
	    		dirName = value.dirList.get(i).substring(43, value.dirList.get(i).length());
	    		fileName = value.keyList.get(i).substring(0,value.keyList.get(i).length()-4);
		    	context.write(new Text(dirName+"/"+fileName), value);
	    		}
	    	}
	    	catch(Exception e){
	    		log.info(e);
	    	}
	    }
	}
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.out.println(otherArgs[1]);
        if (otherArgs.length != 2) {
    		System.err.println("Usage: warning count <in> <out>");
    		System.exit(2);
    	}
	  Job job = new Job(conf, "imageformats");
	  job.setJarByClass(ImagePDF.class);
	  job.setMapperClass(PDFMapper.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(KUPDF.class);
	  job.setInputFormatClass(InputFormatImage.class);
	  job.setOutputFormatClass(OutputFormatImage.class);
	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}