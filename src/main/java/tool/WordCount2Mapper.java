package tool;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCount2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //분석할 파일의 한 줄 값
        String line = value.toString();

        //단어 빈도수 구현은 공백을 기준으로 단어로 구분함
        //분석할 할 줄 내용을 공백으로 나눔
        //word 변수는 공백을 나눠진 단어가 들어감
        for (String word : line.split("\\W+")) {

            //word 변수에 값이 있다면..
            if (word.length() > 0) {

                //suffle and sort로 데이터를 전달하기
                //전달하는 값은 단어와 빈도스(1)를 전달함
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
