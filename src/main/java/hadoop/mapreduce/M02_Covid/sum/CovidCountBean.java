package hadoop.mapreduce.M02_Covid.sum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO 自定义对象作为数据类型在MapReduce
 *                   中传递，别忘了现Hadoop的序列化机制
 *                   即实现Writable接口，如果对对象还有
 *                   比较的需求则可以实现WritableComparable接口
 * create time: TODO 2021/10/3 21:08
 */
public class CovidCountBean implements Writable{
    private long cases;//确诊病例数
    private long deaths;//死亡病例数

    //有参构造
    public CovidCountBean(long cases, long deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }

    //无参构造
    public CovidCountBean() {

    }

    //自己封装set方法，用于对象属性赋值
    public void set(long cases, long deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }


    @Override
    public String toString() {
        return cases+"\t"+deaths;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    public long getDeaths() {
        return deaths;
    }

    public void setDeaths(long deaths) {
        this.deaths = deaths;
    }


    // todo ！！！！！！！！！！！！！！！！！模板代码，基本下次可以照抄：！！！！！！！！！！！！！！！！
    @Override
    /**
     * @author: Suofen
     * description: TODO 实现序列化方法
     *                   将哪些字段转化为字节流
     * create time: TODO 2021/10/3 21:15
     *
     * @Param: dataOutput
     * @return void
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(cases);
        dataOutput.writeLong(deaths);
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO  实现反序列化方法
     *      *             将哪些字节流转化为对象的属性
     *
     *      todo： 注意 反序列化顺序和序列化一致
     *             先进先出原则
     * create time: TODO 2021/10/3 21:15
     *
     * @Param: dataInput
     * @return void
     */
    public void readFields(DataInput dataInput) throws IOException {
        //将读取到的字节流（先进的先处理完先出来，所以第一个出来的是cases）转化为数值赋值给对应的属性
        this.cases = dataInput.readLong();
        this.deaths = dataInput.readLong();
    }


}
