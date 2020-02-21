import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JOB8_1 {
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private IntWritable num = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            num.set(Integer.parseInt(record[1]));
            context.write(one, num);
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>{

        private IntWritable mean = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            int count = 0;
            for(IntWritable value : values){
                sum += value.get();
                count++;
            }
            mean.set(sum/count);
            context.write(mean, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JOB8_1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/sum_friend"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/mean"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
