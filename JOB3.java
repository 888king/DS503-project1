import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JOB3 {

    public static class Map1 extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text page = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            page.set(record[2]);
            context.write(page, one);
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable total_num = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            total_num.set(sum);
            context.write(key, total_num);
        }
    }

    public static class KeyComparator extends WritableComparator{
        protected KeyComparator(){
            super(IntWritable.class, true);
        }
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }
    }

    public static class Map2 extends Mapper<Object, Text, IntWritable, Text>{

        private IntWritable num = new IntWritable();
        private Text page = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(" & ");
            page.set(record[0]);
            int sum = Integer.parseInt(record[1]);
            num.set(sum);
            context.write(num, page);
        }
    }

    public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable>{

        public int N = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text value : values){
                if(N < 10) {
                    context.write(value, key);
                    N++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " & ");
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(JOB3.class);
        job1.setMapperClass(Map1.class);
        job1.setCombinerClass(Reduce1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/user/mqp/AccessLog.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/user/mqp/sum/"));

        Job job2 = Job.getInstance(conf);
        job2.setSortComparatorClass(KeyComparator.class);
        job2.setJarByClass(JOB3.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/user/mqp/sum/"));
        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/user/mqp/output3/"));
        if (job1.waitForCompletion(true)){
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
