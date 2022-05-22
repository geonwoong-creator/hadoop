package mongo.conn;

import com.mongodb.MongoCredential;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;

@Getter
public class MongoDBConnection {

    private MongoDatabase mongoDB;
    private MongoClient mongoClient;

    //생성자를 통해, 객체 생성시 자동으로 메모리에 올리도록 사용
    public  MongoDBConnection() {

        String hostName = "192.168.105.128"; //MongoDB 접속할 IP주소
        int port = 27017; //MongoDB 접속할 포트
        String userName = "myUser" ;//MongoDB 아이디
        String password = "1234";
        String db = "MyDB";

        //MongoDB접속
       mongoClient = new MongoClient(hostName, port);

        //MongoDB 접속정보 설정(아이디, DB, 패스워드)
        MongoCredential.createCredential(userName, db, password.toCharArray());

        //데이터저장 및 삭제할 DB 설정
        mongoDB = mongoClient.getDatabase(db);
    }
}
