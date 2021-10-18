package hadoop.mapreduce.M02_Covid.fenzu;

;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author: Suofen
 * description: TODO  MapReduce中自定义分组规则的类
 * create time: TODO 2021/10/5 14:47
 *
 * @Param: null
 * @return
 */
public class CovidFenzuComparator extends WritableComparator {

    /**
     * @author: Suofen
     * description: TODO  模板代码允许创建指定对象实例
     * create time: TODO 2021/10/5 14:53
     */
    protected CovidFenzuComparator(){
        super(CovidBean.class,true);
    }
    @Override
    /**
     * @author: Suofen
     * description: TODO
     * create time: TODO 2021/10/5 14:52
     *
     * @Param: a
     * @Param: b
     * @return int
     */
    public int compare(WritableComparable a, WritableComparable b) {
        //类型转化
        CovidBean aBean = (CovidBean) a;
        CovidBean bBean = (CovidBean) b;
        //TODO : 本需求中分组规则是字需要前后两个的state州一样
        //       就应该分为同一组
        //       compare 方法 返回 0 MapReduce就认为两个相等分为一组，
        //       默认不返回0则两个不相同，两个不分为一组
        return aBean.getState().compareTo(bBean.getState());
    }
}
