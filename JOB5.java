import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class JOB5 {
    public static class Map extends Mapper<Object, Text, Text, Text>{

        private Text ID = new Text();
        private Text access = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            ID.set(record[1]);
            access.set(record[2]);
            context.write(ID, access);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            Set<String> hash_table = new HashSet<String>();
            for(Text value : values){
                hash_table.add(value.toString());
                sum++;
            }
            result.set("" + sum + "\t" + hash_table.size());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();

        Job job = Job.getInstance(conf1);
        job.setJarByClass(JOB5.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/AccessLog.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/output5/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
