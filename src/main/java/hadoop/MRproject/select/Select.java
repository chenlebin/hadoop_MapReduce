package hadoop.MRproject.select;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Select extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        //��������������
        Configuration conf = new Configuration();

        //ʹ�ù�����ToolRunner���ύ����
        int status = ToolRunner.run(conf, new Select(),args);
        //�˳��ͻ���
        System.exit(status);
    }

    public int run(String[] args) throws Exception {
        //����Job��ҵ��ʵ��
        Job job = Job.getInstance(this.getConf(), Select.class.getSimpleName());

        //TODO ����job����
        //����MapReduce����
        job.setJarByClass(Select.class);

        //���ñ���MapReduce�����mapper��Reduce��
        job.setMapperClass(SelectMapper.class);
        job.setReducerClass(SelectReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //���������������
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        //���ñ�����ҵ����������·�����������·��
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //Ĭ�����  FileInputFormat FileOutputFormat
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //�ж����·���Ƿ��ѽ����ڣ����������ɾ��
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);//rm -rf
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @author: Suofen
     * description: TODO ���Ǿ�����ַ���ƴ��
     * create time: TODO 2021/10/24 20:00
     *
     * @Param: null
     * @return
     */
    static class SelectMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text OutKey = new Text();
        Text OutValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            //������ʽ��
            //1,zhangsan,18,beijing
            OutKey.set(split[3]);
            OutValue.set(split[0]+','+split[1]+','+split[2]);
            //�������
            //System.out.println(OutKey.toString()+OutValue.toString());
            context.write(OutKey,OutValue);
        }
    }


    /**
     * @author: Suofen
     * description: TODO ��key��value�����ж�
     *                   where ��ַ = '�Ϻ�' and age > 25
     * create time: TODO 2021/10/24 20:03
     *
     * @Param: null
     * @return
     */
    static class SelectReducer extends Reducer<Text,Text,NullWritable,Text>{
        Text OutValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //1,zhangsan,18,beijing����
            if(key.toString().equals("shanghai")){
                for (Text value:values) {
                    //ʹ��Integer.parseInt()��String����ǿתΪint����
                    String[] split = value.toString().split(",");
                    //�������
                    //System.out.println("=============================");
                    //System.out.println(split[2]);
                    if(Integer.parseInt(split[2])>25){
                        OutValue.set(value.toString()+","+key.toString());
                        context.write(NullWritable.get(),OutValue);
                    }
                }
            }
        }
    }
}
