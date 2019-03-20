import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.util.Stack;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class JSONRecordReader extends RecordReader<Text, Text> {
    private StringBuffer currKey=new StringBuffer();
    private StringBuffer currValue=new StringBuffer();
    private StringBuffer prevKey=new StringBuffer();
    private String content;
    private Stack<Character> s=new Stack();
    private Text key,val;
    private boolean keyParsing=false, valueParsing=false;
    private int offset=0;
    private FSDataInputStream fsdis;
    private FileSystem fis;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        // your code here
        // the code here depends on what/how you define a split....
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        fis = path.getFileSystem(context.getConfiguration());
        fsdis = fis.open(path);
        byte[] bs = new byte[fsdis.available()];
        fsdis.read(bs);
        content=new String(bs);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        /*
            The code will parse multi level json
            So if  my json is
                    {
                      "name":"nikhil",
                      "address":"Brooklyn",
                      "college":{
                        "name":"NYU",
                        "address":"Brooklyn",
                        "departments":{
                          "name":"Computer Science",
                          "HOD":"Prof. Leung",
                          "address":{
                            "location":"Brooklyn"
                          }
                        }
                      }
                    }
             Then the input to the mapper will be
             key="name" and value="nikhil"
             key="college.name" and value="NYU"
             key="college.departments.address" and value="Brooklyn"
         */
        for(;offset<content.length();offset++){
            Character c= content.charAt(offset);
            if(c=='{'){
                s.push(c);
                if(s.size()>1){
                    prevKey=new StringBuffer(prevKey.toString()+currKey.toString());
                    prevKey.append('.');
                    currKey=new StringBuffer();
                }
            }
            else if(c=='}'){
                s.pop();
                int end=prevKey.toString().lastIndexOf(".");
                if(end>0){
                    prevKey=new StringBuffer(prevKey.toString().substring(0,end));
                }
                else
                    prevKey=new StringBuffer();
            }
            else if(c=='\"' && currKey.length()==0 && !keyParsing)
                keyParsing=true;
            else if(c!='\"' && keyParsing && currValue.length()==0)
                currKey.append(c);
            else if(c=='\"' && currKey.length()!=0 && keyParsing && currValue.length()==0)
                keyParsing=false;
            else if(c=='\"' && !keyParsing && !valueParsing && currKey.length()!=0 && currValue.length()==0)
                valueParsing=true;
            else if(c!='\"' && !keyParsing && valueParsing && currKey.length()!=0)
                currValue.append(c);
            else if(c=='\"'&& !keyParsing && valueParsing && currKey.length()!=0 && currValue.length()!=0)
                valueParsing=false;

            if(!keyParsing && !valueParsing && currKey.length()!=0 && currValue.length()!=0){
                key=new Text(prevKey.toString()+currKey.toString());
                val=new Text(currValue.toString());
                currKey=new StringBuffer();
                currValue=new StringBuffer();
                offset++;
                return true;
            }
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // let's ignore this one for now
        return offset>=content.length() ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // your code here
        // the code here depends on what/how you define a split....
        try {
            fsdis.close();
            fis.close();
        } catch (Exception e) {
            System.out.println("Not able to close the streams");
        }
    }
}
