package ceph.s3.file.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
import ceph.s3.config.AwzS3Config;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class AwzS3Util {
    @Resource
    private AwzS3Config injectAwzS3Config;
    private static AwzS3Config awzS3Config;
    private static AmazonS3 amazonS3;

    @PostConstruct
    public void init() {
        // 从注入的 AwzS3Config 中获取 AWS S3 相关配置
        awzS3Config = this.injectAwzS3Config;
        // 创建 AWS 认证对象
        AWSCredentials credentials = new BasicAWSCredentials(awzS3Config.getAccessKey(), awzS3Config.getSecretKey());
        // 创建 AWS 认证提供程序
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        // 设置 S3 客户端的终端配置
        AwsClientBuilder.EndpointConfiguration endpointConfig =
                new AwsClientBuilder.EndpointConfiguration(awzS3Config.getUrl(), Regions.CN_NORTH_1.getName());
        // 创建客户端配置对象
        ClientConfiguration config = new ClientConfiguration();
        // 设置签名类型为 S3SignerType
        config.setSignerOverride("S3SignerType");
        // 设置通信协议为 HTTP
        config.setProtocol(Protocol.HTTP);
        // 禁用 Expect: 100-continue 头
        config.withUseExpectContinue(false);
        // 禁用 Socket 代理
        config.disableSocketProxy();
        // 创建 Amazon S3 客户端
        amazonS3 = AmazonS3Client.builder()
                .withEndpointConfiguration(endpointConfig)
                .withClientConfiguration(config)
                .withCredentials(credentialsProvider)
                .disableChunkedEncoding()
                .withPathStyleAccessEnabled(true)
                .withForceGlobalBucketAccessEnabled(true)
                .build();
    }

    public static boolean createBucket(String bucket) {
        if (StringUtils.isNullOrEmpty(bucket)) {
            throw new IllegalArgumentException("桶名称不能为空!");
        }
        try {
            amazonS3.createBucket(bucket);
        } catch (SdkClientException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteBucket(String bucket) {
        if (StringUtils.isNullOrEmpty(bucket)) {
            throw new IllegalArgumentException("桶名称不能为空!");
        }
        try {
            amazonS3.deleteBucket(bucket);
        } catch (SdkClientException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean uploadByBlock(MultipartFile file, String bucket) {
        // 检查文件是否为空，如果为空，则抛出异常
        if (Objects.isNull(file)) {
            throw new IllegalArgumentException("文件不能为空!");
        }
        // 获取文件名
        String fileName = file.getOriginalFilename();
        // 如果未指定存储桶，则使用默认存储桶
        bucket = StringUtils.isNullOrEmpty(bucket) ? awzS3Config.getBucket() : bucket;
        // 初始化分段上传请求
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucket, fileName);
        // 发起分段上传，并获取初始化结果
        InitiateMultipartUploadResult initiateResult = amazonS3.initiateMultipartUpload(initiateRequest);
        // 获取文件大小和分段大小
        long contentLength = file.getSize();
        long partSize = 5 * 1024 * 1024; // 分段大小为5MB
        int totalParts = (int) Math.ceil((double) contentLength / partSize);
        // 用于存储每个分段的 ETag
        List<PartETag> partsETags = new ArrayList<>();

        try (InputStream inputStream = file.getInputStream()) {
            // 循环上传每个分段
            for (int i = 0; i < totalParts; i++) {
                // 计算当前分段的起始位置和大小
                long filePosition = i * partSize;
                long partSizeTemp = Math.min(partSize, contentLength - filePosition);
                // 读取分段数据
                byte[] partData = new byte[(int) partSizeTemp];
                inputStream.read(partData);
                // 创建分段上传请求
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucket)
                        .withKey(fileName)
                        .withUploadId(initiateResult.getUploadId())
                        .withPartNumber(i + 1)
                        .withPartSize(partSizeTemp)
                        .withInputStream(new ByteArrayInputStream(partData));
                // 上传分段并获取结果
                UploadPartResult uploadResult = amazonS3.uploadPart(uploadRequest);
                // 将分段的 ETag 添加到列表中
                partsETags.add(uploadResult.getPartETag());
            }
            // 完成分段上传请求
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucket, fileName, initiateResult.getUploadId(), partsETags);
            // 完成分段上传并获取结果
            CompleteMultipartUploadResult completeResult = amazonS3.completeMultipartUpload(completeRequest);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static ResponseEntity<byte[]> downloadByName(String bucket, String fileName) throws IOException {
        // 检查存储桶名称是否为空，如果为空，则抛出异常
        if (StringUtils.isNullOrEmpty(bucket)) {
            throw new IllegalArgumentException("存储桶名称不能为空!");
        }
        // 如果未指定存储桶，则使用默认存储桶
        bucket = StringUtils.isNullOrEmpty(bucket) ? awzS3Config.getBucket() : bucket;
        // 创建获取对象请求
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, fileName);
        // 从 Amazon S3 中获取对象
        S3Object s3Object = amazonS3.getObject(getObjectRequest);
        // 获取对象的输入流
        S3ObjectInputStream objectInputStream = s3Object.getObjectContent();
        // 将输入流转换为字节数组
        byte[] bytes = IOUtils.toByteArray(objectInputStream);
        // 对文件名进行 URL 编码，并替换空格为 %20
        String showFileName = URLEncoder.encode(fileName, "UTF-8").replaceAll("\\+", "%20");
        // 设置 HTTP 头信息
        HttpHeaders httpHeaders = new HttpHeaders();
        // 设置内容类型为二进制流
        httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        // 设置内容长度
        httpHeaders.setContentLength(bytes.length);
        // 设置文件下载时的文件名，并指定为附件
        httpHeaders.setContentDispositionFormData("attachment", showFileName);
        // 返回带有字节数组、HTTP 头和状态码的 ResponseEntity 对象
        return new ResponseEntity<>(bytes, httpHeaders, HttpStatus.OK);
    }

    public static boolean deleteFile(String bucket, String fileName) {
        if (StringUtils.isNullOrEmpty(fileName)) {
            throw new IllegalArgumentException("文件名称不能为空!");
        }
        bucket = StringUtils.isNullOrEmpty(bucket) ? awzS3Config.getBucket() : bucket;
        try {
            amazonS3.deleteObject(bucket, fileName);
        } catch (SdkClientException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static ObjectMetadata getFileInfo(String bucket, String fileName) {
        // 检查文件名是否为空，如果为空，则抛出异常
        if (StringUtils.isNullOrEmpty(fileName)) {
            throw new IllegalArgumentException("文件名称不能为空!");
        }
        // 如果未指定存储桶，则使用默认存储桶
        bucket = StringUtils.isNullOrEmpty(bucket) ? awzS3Config.getBucket() : bucket;
        // 初始化对象元数据为 null
        ObjectMetadata objectMetadata = null;
        try {
            // 获取对象的元数据信息
            objectMetadata = amazonS3.getObjectMetadata(bucket, fileName);
        } catch (SdkClientException e) {
            // 捕获 AWS SDK 客户端异常，并打印异常信息
            e.printStackTrace();
        }
        // 返回对象的元数据信息
        return objectMetadata;
    }
}
