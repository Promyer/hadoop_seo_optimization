import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HostSort extends Configured implements Tool {

    int minclicks = 0;

    public static String getHost(String url){
        if(url == null || url.length() == 0)
            return "";

        int doubleslash = url.indexOf("//");
        if(doubleslash == -1)
            doubleslash = 0;
        else
            doubleslash += 2;

        int end = url.indexOf('/', doubleslash);
        end = end >= 0 ? end : url.length();

        int port = url.indexOf(':', doubleslash);
        end = (port > 0 && port < end) ? port : end;

        return url.substring(doubleslash, end);
    }
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new HostSort(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class HostQueryPartitioner extends Partitioner<TextTextPair, Text> {
        @Override
        public int getPartition(TextTextPair key, Text val, int numPartitions) {
            return Math.abs(key.getUrl().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextTextPair.class, true /* десериализовывать ли объекты (TextTextPair) для compare */);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextTextPair)a).compareTo((TextTextPair)b);
        }
    }

    public static class MDStationGrouper extends WritableComparator {
        protected MDStationGrouper() {
            super(TextTextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            Text a_first = ((TextTextPair)a).getUrl();
            Text b_first = ((TextTextPair)b).getUrl();
            return a_first.compareTo(b_first);
        }
    }

    public static class HostMapper extends Mapper<LongWritable, Text, TextTextPair, Text>
    {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            Text host = new Text(getHost(items[1]));

            context.write(new TextTextPair(host, new Text(items[0])), new Text(items[0]));

        }
    }


    public static class HostReducer extends Reducer<TextTextPair, Text, TextTextPair, Text> {
        @Override
        protected void reduce(TextTextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text best_query = key.getSecond();
            Text cur_query = key.getSecond();
            int best_len = 1;
            int cur_len = 0;

            for(Text w:values) {
                if (w == cur_query){
                    cur_len ++;
                }
                else {
                    if (cur_len > best_len)
                    {
                        best_len = cur_len;
                        best_query = cur_query;
                    }
                    cur_len = 1;
                    cur_query = w;
                }
            }
            if (cur_len > best_len) {
                best_len = cur_len;
                best_query = cur_query;
            }
            String result = (key.getUrl()).toString() + "\t" + (best_query).toString();
            if (minclicks <= best_len){
                context.write(new TextTextPair(key.getUrl(), best_query), new Text(String.valueOf(best_len)));
            }
        }
    }

    Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(HostSort.class);
        job.setJobName(HostSort.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(HostMapper.class);
        job.setReducerClass(HostReducer.class);

        job.setPartitionerClass(HostQueryPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(MDStationGrouper.class);

        // выход mapper-а != вывод reducer-а, поэтому ставим отдельно
        job.setMapOutputKeyClass(TextTextPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}