import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JOB4 {

    public static class Map1 extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text ID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            ID.set(record[2]);
            context.write(ID, one);
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable sum = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int result = 0;
            for(IntWritable value : values){
                result += value.get();
            }
            sum.set(result);
            context.write(key, sum);
        }
    }

    public static class Map2 extends Mapper<Object, Text, Text, Text>{

        private Text info = new Text();
        private Text ID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            ID.set(record[0]);
            info.set(record[1]);
            context.write(ID, info);
        }
    }
  
    public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
        private Text name = new Text();
        private Text num = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            char first;
            String[] temp = new String[2];

            for(Text value : values) {
                first = value.toString().charAt(0);
                if (first > '9') {
                    temp[0] = value.toString();
                }
                else if(first <= '9'){
                    temp[1] = value.toString();
                }
            }
            //System.out.println(temp);
            name.set(temp[0]);
            num.set(temp[1]);
            System.out.println(name);
            context.write(name, num);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf1 = new Configuration();
        conf1.set("mapred.textoutputformat.separator", ",");

        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(JOB4.class);
        job1.setMapperClass(Map1.class);
        job1.setCombinerClass(Reduce1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/user/mqp/AllFriends.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/user/mqp/sum_friend/"));

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(JOB4.class);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job2, new Path("hdfs://localhost:9000/user/mqp/MyPage.csv"), TextInputFormat.class, Map2.class);
        MultipleInputs.addInputPath(job2, new Path("hdfs://localhost:9000/user/mqp/sum_friend/"), TextInputFormat.class, Map2.class);
        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/user/mqp/output4/"));

        if (job1.waitForCompletion(true)) {
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
