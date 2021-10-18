package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OutScCompartor extends WritableComparator {
    //允许创建UsaScBean实例
    protected OutScCompartor(){
        super(UsaScBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //类型转化
        UsaScBean aBean = (UsaScBean) a;
        UsaScBean bBean = (UsaScBean) b;
        //TODO : 本需求中分组规则是需要前后两个的county县一样
        //       就应该分为同一组
        //       compare 方法 返回 0 MapReduce就认为两个相等分为一组，
        //       默认不返回0则两个不相同，两个不分为一组
        return aBean.getCounty().compareTo(bBean.getCounty());
    }
}
