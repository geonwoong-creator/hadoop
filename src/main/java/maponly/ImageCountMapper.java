package maponly;


import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ImageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\"");

        //분석할 파일의 한줄 값
        if (fields.length > 1) {
            String request = fields[1];

            if (fields.length > 1) {

                String fileName = fields[1].toLowerCase();

                if (fileName.endsWith(".jpg")) {
                    context.getCounter("imageCount", "jpg").increment(1);
                } else if (fileName.endsWith(".gif")) {
                    context.getCounter("imageCount", "gif").increment(1);
                } else {
                    context.getCounter("imageCount", "other").increment(1);
                }
        }
    }


    }
}
