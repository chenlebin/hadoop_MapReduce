package hadoop.mapreduce.M02_Covid.fenqu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @author: Suofen
 * description: TODO 自定义分区规则的类
 *                   本案例当中为以state州
 *                   进行分区
 * create time: TODO 2021/10/4 20:22
 *
  * @Param: null
 * @return
 */
public class StateFenqu extends Partitioner<Text,Text> {
    //数据字典模拟美国各州的数据字典，实际中可以从redis进行读取加载，如果数据不大也可以使用一个字典来存储
    public static  HashMap<String,Integer> statemap = new HashMap<String,Integer>();

    //美国55个州的字典
    static {
        statemap.put("Alabama",0);
        statemap.put("Alaska",1);
        statemap.put("Arizona",2);
        statemap.put("Arkansas",3);
        statemap.put("California",4);
        statemap.put("Colorado",5);
        statemap.put("Connecticut",6);
        statemap.put("Delaware",7);
        statemap.put("District of Columbia",8);
        statemap.put("Florida",9);
        statemap.put("Georgia",10);
        statemap.put("Guam",11);
        statemap.put("Hawaii",12);
        statemap.put("Idaho",13);
        statemap.put("Illinois",14);
        statemap.put("Indiana",15);
        statemap.put("Iowa",16);
        statemap.put("Kansas",17);
        statemap.put("Kentucky",18);
        statemap.put("Louisiana",19);
        statemap.put("Maine",20);
        statemap.put("Maryland",21);
        statemap.put("Massachusetts",22);
        statemap.put("Michigan",23);
        statemap.put("Minnesota",24);
        statemap.put("Mississippi",25);
        statemap.put("Missouri",26);
        statemap.put("Montana",27);
        statemap.put("Nebraska",28);
        statemap.put("Nevada",29);
        statemap.put("New Hampshire",30);
        statemap.put("New Jersey",31);
        statemap.put("New Mexico",32);
        statemap.put("New York",33);
        statemap.put("North Carolina",34);
        statemap.put("North Dakota",35);
        statemap.put("Northern Mariana Islands",36);
        statemap.put("Ohio",37);
        statemap.put("Oklahoma",38);
        statemap.put("Oregon",39);
        statemap.put("Pennsylvania",40);
        statemap.put("Puerto Rico",41);
        statemap.put("Rhode Island",42);
        statemap.put("South Carolina",43);
        statemap.put("South Dakota",44);
        statemap.put("Tennessee",45);
        statemap.put("Texas",46);
        statemap.put("Utah",47);
        statemap.put("Vermont",48);
        statemap.put("Virgin Islands",49);
        statemap.put("Virginia",50);
        statemap.put("Washington",51);
        statemap.put("West Virginia",52);
        statemap.put("Wisconsin",53);
        statemap.put("Wyoming",54);
    }
    @Override
    /**
     * @author: Suofen
     * description: TODO  实现方法，只要getPartition返回的int一样，
     *                    数据就会被分到同一个分区，所谓的数据分区指的
     *                    就是数据到同一个ReduceTask处理
     * create time: TODO 2021/10/4 20:26
     *
     * @Param: key   state ,州
     * @Param: value 一行文本数据：2020/1/21,Snohomish,Washington,1,0
     * @Param: numPartitions
     * @return int
     */
    public int getPartition(Text key,Text value, int numPartitions) {
        Integer code = statemap.get(key.toString());
        if(code != null)
            return code;
        return 55;
    }
}
