import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndex {

    public static class InvertedIndexMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();
        // private final static Text docId = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            // Get File Name ex) 65600173.txt
            FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            // Get DocId ex) 65600173
            String docId = fileName.replace(".txt","");

            // Get each line for documents
            String line = value.toString();

            // Divide lines into tokens
            StringTokenizer itr = new StringTokenizer(line);

            // Convert into lowercase
            // StringTokenizer itr = new StringTokenizer(line.toLowerCase());

            // Map output is (word, DocId)
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                // The word of DocId in documents should not be considered and counted for word.
                if (docId.equals(nextToken)) {
                    continue;
                }
                word.set(nextToken);
                output.collect(word, new Text(docId));
            }
        }
    }



    public static class InvertedIndexReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

            // While the value is exist in line
            while (values.hasNext()) {
                // Value for each line is docId (key=word, value=docId)
                String docId = values.next().toString();

                // Get count for each docId from hashMap
                Integer currentCount = hashMap.get(docId);

                // Update count for each docId to hashMap
                if (currentCount == null) {
                    hashMap.put(docId, 1);
                }
                else {
                    currentCount = currentCount + 1;
                    hashMap.put(docId, currentCount);
                }
            }
            // output.collect(key, new Text(hashMap.toString()));

            // Set output format
            boolean isFirst = true;
            StringBuilder toReturn = new StringBuilder();
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if(!isFirst) {
                    toReturn.append("\t");
                }
                isFirst = false;
                toReturn.append(entry.getKey()).append(":").append(entry.getValue());
            }

            output.collect(key, new Text(toReturn.toString()));
        }
    }


    /**
     * The actual main() method for our program; this is the
     * "driver" for the MapReduce job.
     */
    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(InvertedIndex.class);

        conf.setJobName("InvertedIndex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(InvertedIndexMapper.class);
        conf.setReducerClass(InvertedIndexReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}