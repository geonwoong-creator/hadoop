package ip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{

        //ip별 빈도스를 계산하기 위한 변수
        int ipCount = 0;

        for (IntWritable value : values) {

            ipCount += value.get();
        }

        context.write(key, new IntWritable(ipCount));
    }
}
