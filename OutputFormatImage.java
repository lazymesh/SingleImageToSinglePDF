import com.lowagie.text.Document;
import com.lowagie.text.pdf.PdfCopy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputFormatImage extends FileOutputFormat<Text, KUPDF> {
	TaskAttemptContext job;

	@Override
	public RecordWriter<Text, KUPDF> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		this.job = job;
		
	    return new PDFWriter(job);
		
	}
	
	public class PDFWriter extends RecordWriter<Text, KUPDF> {
		private final Log log = LogFactory.getLog(PDFWriter.class);
		TaskAttemptContext job;
		Path file;
		FileSystem fs;
		int i = 0;
        
		PDFWriter(TaskAttemptContext job){
			this.job = job;
        }
        
        @Override
        public void close(TaskAttemptContext context) throws IOException {
        	//doc.close();
        }
        
        //get the names of the image and directories and pass them as output
        @Override
        public synchronized void write(Text key, KUPDF value) throws IOException, InterruptedException {
        	Configuration conf = job.getConfiguration();
        	Path name = getDefaultWorkFile(job, null);
        	String outfilepath = name.toString();
        	String keyname = key.toString();
        	Path file =new Path((outfilepath.substring(0,outfilepath.length()-16))+ keyname+".pdf");
        	FileSystem fs = file.getFileSystem(conf);
        	FSDataOutputStream fileOut = fs.create(file, false);
        	writeDocument(value, fileOut);
        }
        
        //write the pdf files passed by reader
        public void writeDocument(KUPDF o, FSDataOutputStream out) throws IOException{
        	
        		try{
        			Document doc = new Document();
        			PdfCopy copy = new PdfCopy(doc, out);
        			int inc = 0;
        			log.info(o.reader.getFileLength());
        			doc.open();
        			while(inc<o.reader.getNumberOfPages()){
        				inc++;
        				copy.addPage(copy.getImportedPage(o.reader, 1));
        			}
        			doc.close();
        		}
        		catch(Exception e){
        			log.info("exception : "+e);
        		}   		
        }
	}
}