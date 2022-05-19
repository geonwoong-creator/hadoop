package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TimeLogReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        //ip별 빈도스를 계산하기 위한 변수
        int ipCount = 0;

        for (Text value : values) {
            ipCount++;
        }

        context.write(key, new IntWritable(ipCount));
    }
}

