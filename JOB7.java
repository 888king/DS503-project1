import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class JOB7 {

    public static class Map1 extends Mapper<Object, Text, Text, Text>{

        private Text ID = new Text();
        private Text Access = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            ID.set(record[1]);
            Access.set("A" + record[2]);
            context.write(ID, Access);
        }
    }

    public static class Map2 extends Mapper<Object, Text, Text, Text>{

        private Text ID = new Text();
        private Text friend = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            ID.set(record[1]);
            friend.set("F" + record[2]);
            context.write(ID, friend);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable>{

        Set<String> Accesses = new HashSet<String>();
        Set<String> Friends = new HashSet<String>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text value : values){
                if(value.toString().charAt(0) == 'F'){
                    Friends.add(value.toString().substring(1));
                }
                else {
                    Accesses.add(value.toString().substring(1));
                }
            }
            for(String friend : Friends){
                if(!Accesses.contains(friend)){
                    context.write(key, NullWritable.get());
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JOB7.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/AccessLog.csv"), TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/AllFriends.csv"), TextInputFormat.class, Map2.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/output7"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
