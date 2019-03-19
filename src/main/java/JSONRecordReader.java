import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.util.Stack;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
 public class JSONRecordReader extends RecordReader<LongWritable, Text> {

    private LineRecordReader reader = new LineRecordReader();
    private StringBuffer currValue = new StringBuffer();
    private Stack<Character> stack = new Stack();
    private String line = new String();
    private int lineLength = 0;
    private int position = 0;
    private long currPos;
    private  boolean isFinished = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
       reader.initialize(inputSplit, context);
       lineLength = 0;
       line = null;
       stack = new Stack();
       isFinished = false;
       currPos = 0;
    }

    //set the next json object in currValue
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        currValue = currValue.delete(0,currValue.length());
        stack = new Stack();
        while (true) {
            if (position >= lineLength) {
                if (reader.nextKeyValue()) {
                    line = reader.getCurrentValue() != null ? reader.getCurrentValue().toString() : null;
                    position = 0;
                    lineLength = line.length();
                } else {
                    line = null;
                }
            }
            if (line == null) {
                isFinished = true;
                currValue.delete(0,currValue.length());
                return false;
            }
            Character temp, top;
            while (position < lineLength) {
                temp = line.charAt(position);
                position += 1;
                currPos += 1;
                if (Character.isWhitespace(temp))
                    continue;
                currValue.append(temp);
                if (stack.empty() && !(temp == '{' || temp == '[')) {
                    return false;
                }
                if (temp == '{' || temp == '[') {
                    stack.push(temp);
                } else {
                    if (temp == ']') {
                        top = stack.peek();
                        if (top == '[') {
                            stack.pop();
                            if (stack.empty()) {
                                return true;
                            }
                        } else {
                            currValue.delete(0, currValue.length());
                            return false;
                        }
                    }
                    if (temp == '}') {
                        top = stack.peek();
                        if (top == '{') {
                            stack.pop();
                            if (stack.empty()) {
                                return true;
                            }
                        } else {
                            currValue.delete(0, currValue.length());
                            return false;
                        }
                    }
                }
            }
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey(); //new LongWritable(currPos - currValue.length());
    }

   @Override
   public Text getCurrentValue() throws IOException,
           InterruptedException {
      return new Text(currValue.toString());
   }

   @Override
   public float getProgress()
           throws IOException, InterruptedException {
      return reader.getProgress();
   }
    @Override
    public synchronized void close() throws IOException {
        reader.close();
    }
}
