import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JOB1 {
    public static class Map extends Mapper<Object, Text, NullWritable, Text> {

        private Text info = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            //System.out.println(record[2]);
            if(record[2].equals("China")){
                info.set(record[1] + "," + record[4]);
                context.write(NullWritable.get(), info);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(JOB1.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/MyPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/output1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
