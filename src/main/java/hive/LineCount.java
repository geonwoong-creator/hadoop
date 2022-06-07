package hive;

import hive.conn.HiveConn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LineCount {

    public static void main(String[] args) throws SQLException {

        Connection conn = null;

        try {
            //Hive 접속하기
            conn = HiveConn.getDBConnection();

            //Hive 실행을 위한 SQL객체
            PreparedStatement pstmt = null;

            //Hive의 comedies 데이터의 전체 라인수 계산하기
            String sql = "select count(line_data) as cnt from hivedb.comedies";

            //sql문자열을 Hive로 실행할 수 있는 SQL 쿼리로 변환하기
            pstmt = conn.prepareStatement(sql);

            //쿼리 실행 후 결과 가져오기
            ResultSet rs = pstmt.executeQuery(sql);

            //COUNT 함수를 수행했기 때문에 반드시 결과값은 1개 레코드 나옴
            //데이터가 없어도 0이 출력됨
            if (rs.next()) {

                //select 조회 결과를 가져오기
                String cnt = rs.getString("cnt");

                System.out.println("cnt : " + cnt);
            }
            pstmt = null;
        } catch (Exception e) {
            System.out.println("에러발생 : " + e);
        } finally {
            //오라클 접속 해제
            HiveConn.dbClose(conn);
        }
    }
}
