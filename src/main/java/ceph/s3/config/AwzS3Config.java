package ceph.s3.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Data
@Configuration
@ConfigurationProperties(prefix = "aws.s3")
public class AwzS3Config {
    private String accessKey;
    private String secretKey;
    private String url;
    private String bucket;
}
