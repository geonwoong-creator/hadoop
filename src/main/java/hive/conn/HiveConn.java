package hive.conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConn {
    /**
     * Hive 연결하기
     */
    public static Connection getDBConnection() throws SQLException {

        Connection conn = null;

        try {

            //hive에 접속하기 위한 드라이버 파일 로드
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            conn = DriverManager.getConnection("jdbc:hive2://192.168.105.129:10000/hivedb", "hadoop", "1234");

            System.out.println("Hive 접속 성공!");
        } catch (ClassNotFoundException e) {
            System.out.println("Hive 접속 실패");
            System.out.println("org.apache.hive.jdbc.HiveDriver 파일을 찾을 수 없습니다.");
            System.out.println("이유 : " + e);
        } catch (Exception e) {
            System.out.println("Hive 접속 실패");
            System.out.println("최종 Exception까지 도착했음");
            System.out.println("이유 : " + e);
    }
        return conn;
}
    public static void dbClose(Connection conn) throws SQLException {
        conn.close();
    }
}
