import org.apache.commons.lang.ObjectUtils;
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

public class JOB8_2 {

    public static class Map2 extends Mapper<Object, Text, Text, NullWritable>{

        private Text ID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            int sum = Integer.parseInt(record[1]);
            ID.set(record[0]);
            if(sum > 100)
                context.write(ID, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JOB8_2.class);
        job.setMapperClass(Map2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/sum_friend"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/output8"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
