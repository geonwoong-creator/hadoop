package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPCount2mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String ip = ""; //로그부터 추출된 ip
        int forCnt = 0; //반복 횟수


        for (String field : line.split("\\W+")) {

            if (field.length() > 0) {

                forCnt++;
                ip += (field + ".");

                if (forCnt == 4) {

                    ip = ip.substring(0, ip.length()-1);

                    context.write(new Text(ip), new IntWritable(1));
                }
            }
        }
    }
}
