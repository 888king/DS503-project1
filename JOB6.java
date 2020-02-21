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

public class JOB6 {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private Text ID = new Text();
        private IntWritable time = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            ID.set(record[1]);
            time.set(Integer.parseInt(record[4]));
            context.write(ID, time);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int max = 0;
            for(IntWritable value : values){
                int temp = value.get();
                if(temp > max)
                    max = temp;
            }
            if(max < 800000)
                context.write(key, NullWritable.get());
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JOB6.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/AccessLog.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/output6"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}