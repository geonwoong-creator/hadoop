package mongo.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;


@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Data
public class AccessLogDTO {

    private String ip; //ip
    private String reqTime; //요청일시
    private String reqMethod; //요청방법
    private String reqURI; // 요청 URI
}
