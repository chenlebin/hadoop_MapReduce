package hadoop.mapreduce.M06_IO.SequenceFile.HeBing;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class SequenceFileHeBing {
    //定义全局变量
    private Configuration configuration = new Configuration();
    private List<String> smallFilePaths = new ArrayList<>();
    /**
     * @author: Suofen
     * description: TODO 定义方法来添加小文件路径
     * create time: TODO 2021/10/17 21:21
     *
      * @Param: null
     * @return
     */
    public void addInputPath(String inputPath) throws Exception {
        File file = new File(inputPath);
        //如果是一个目录，则遍历目录中的所有文件
        if(file.isDirectory()){
            File[] files = FileUtil.listFiles(file);
            for (File sFile: files){
                smallFilePaths.add(sFile.getPath());
                System.out.println("添加小文件路径："+sFile.getPath());
            }
        }
        //如果是一个文件，则直接输出这个文件的
        else {
            smallFilePaths.add(file.getPath());
            System.out.println("添加小文件路径："+file.getPath());
        }
    }

    /**
     * @author: Suofen
     * description: TODO 把smallFilePaths中的小文件遍历读取，然后放入合并的SeqFile容器中
     * create time: TODO 2021/10/18 10:19
     *
      * @Param: null
     * @return
     */
    public void mergeFile(String lj) throws Exception{
        SequenceFile.Writer.Option bigFile = SequenceFile.Writer.file(new Path(lj));
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(BytesWritable.class);
        //构造writer
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,bigFile,keyClass,valueClass);
        //遍历读取小文件，逐个写入SequenceFile
        System.out.println(smallFilePaths);
        Text key= new Text();
        for (String path:smallFilePaths) {
            File file = new File(path);
            long fileSize = file.length();//读取文件的字节数大小
            byte[] fileContent = new byte[(int)fileSize];
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.read(fileContent,0,(int) fileSize);
            String md55tr = DigestUtils.md5Hex(fileContent);
            System.out.println("merge小文件："+path+",md5:"+md55tr);
            key.set(path);
            //吧文件路径作为key，文件内容作为value,放入到SequenceFile中
            //writer.append(key, new BytesWritable(fileContent));
            writer.append(key,new BytesWritable(fileContent));
        }

        writer.hflush();
        writer.close();
    }

    /**
     * @author: Suofen
     * description: TODO 读取大文件中的小文件
     * create time: TODO 2021/10/18 10:31
     *
      * @Param: null
     * @return
     */
    public void readMergedFile(String lj) throws Exception{
        SequenceFile.Reader.Option file = SequenceFile.Reader.file(new Path(lj));
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration,file);
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        while(reader.next(key,value)){
            byte[] bytes = value.copyBytes();
            String md5 = DigestUtils.md5Hex(bytes);
            String content = new String(bytes, Charset.forName("GBK"));
            System.out.println("读取文件:"+key+"md5:"+md5+",+content:"+content);
        }


    }


    /**
     * @author: Suofen
     * description: TODO main方法调用方法实现功能
     * create time: TODO 2021/10/17 21:27
     *
      * @Param: null
     * @return
     */
    public static void main(String[] args) throws Exception {
        //创建类的对象实例
        SequenceFileHeBing seq = new SequenceFileHeBing();
        //todo 合并小文件
        //seq.addInputPath("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce调优\\IO\\SequenceFile\\HeBing\\input");
        //seq.mergeFile("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce调优\\IO\\SequenceFile\\HeBing\\output\\bigfile.seq");

        //todo 读取大文件
        seq.readMergedFile("H:\\hadoop-3.1.4_MapReduce_local_output\\MapReduce调优\\IO\\SequenceFile\\HeBing\\output\\bigfile.seq");
    }
}
