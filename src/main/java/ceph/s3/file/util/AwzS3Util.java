package ceph.s3.file.util;

import com.amazonaws.AmazonServiceException;
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
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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

    public static String uploadOneBlock(MultipartFile file, int position, long blockSize, String bucket) {
        // 检查文件是否为空，如果为空，则返回错误信息
        if (Objects.isNull(file)) {
            return "文件为空";
        }
        // 获取文件名
        String fileName = file.getOriginalFilename();
        // 初始化分段上传请求
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucket, fileName);
        // 发起分段上传，并获取初始化结果
        InitiateMultipartUploadResult initiateResult = amazonS3.initiateMultipartUpload(initiateRequest);
        // 获取文件大小
        long contentLength = file.getSize();
        // 计算分段的起始位置和结束位置
        long start = position * blockSize;
        long end = Math.min((position + 1) * blockSize, contentLength);
        // 计算分段大小
        long partSize = end - start;
        // 用于存储分段的 ETag
        PartETag partETag = null;
        try (InputStream inputStream = file.getInputStream()) {
            // 跳过文件中前面的数据到指定的起始位置
            inputStream.skip(start);
            // 读取分段数据
            byte[] partData = new byte[(int) partSize];
            int bytesRead = inputStream.read(partData);
            // 创建分段上传请求
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(fileName)
                    .withUploadId(initiateResult.getUploadId())
                    .withPartNumber(position + 1)
                    .withPartSize(partSize)
                    .withInputStream(new ByteArrayInputStream(partData));
            // 上传分段并获取结果
            UploadPartResult uploadResult = amazonS3.uploadPart(uploadRequest);
            partETag = uploadResult.getPartETag();
            // 完成分段上传请求
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucket, fileName, initiateResult.getUploadId(), List.of(partETag));
            CompleteMultipartUploadResult completeResult = amazonS3.completeMultipartUpload(completeRequest);
            return completeResult.getKey();
        } catch (IOException | AmazonServiceException e) {
            // 处理异常
            e.printStackTrace();
            return e.getMessage();
        }
    }


    public static ResponseEntity<byte[]> downloadByName(String bucket, String fileName, String localPath) throws IOException {
        if (!StringUtils.hasValue(bucket)) {
            throw new IllegalArgumentException("存储桶名称不能为空!");
        }

        bucket = !StringUtils.hasValue(bucket) ? awzS3Config.getBucket() : bucket;

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, fileName);
        S3Object s3Object = amazonS3.getObject(getObjectRequest);
        S3ObjectInputStream objectInputStream = s3Object.getObjectContent();
        byte[] bytes = IOUtils.toByteArray(objectInputStream);
        // 检查并创建本地路径
        if (!Files.exists(Paths.get(localPath))) {
            Files.createDirectories(Paths.get(localPath));
        }
        // 保存文件到本地路径
        try (FileOutputStream fos = new FileOutputStream(localPath + "/" + fileName)) {
            fos.write(bytes);
        }

        String showFileName = URLEncoder.encode(fileName, "UTF-8").replaceAll("\\+", "%20");
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        httpHeaders.setContentLength(bytes.length);
        httpHeaders.setContentDispositionFormData("attachment", showFileName);

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

    //解压缩sourceBucket里的sourceKey文件，并将解压缩后的文件上传到targetBucket
    public static boolean decompressAndUpload(String sourceBucket, String sourceKey, String targetBucket) {
        try {
            // 从S3获取压缩文件
            S3Object s3object = amazonS3.getObject(new GetObjectRequest(sourceBucket, sourceKey));
            ZipInputStream zis = new ZipInputStream(new BufferedInputStream(s3object.getObjectContent()), Charset.forName("GBK"));
            ZipEntry entry;

            while ((entry = zis.getNextEntry()) != null) {
                String fileName = entry.getName();
                File tempFile = File.createTempFile("decompressed_", fileName);

                // 解压缩文件内容到临时文件
                try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                }

                // 上传解压后的文件到目标存储桶
                amazonS3.putObject(targetBucket, fileName, new FileInputStream(tempFile).toString());
                tempFile.delete(); // 删除临时文件
            }
            zis.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    //读取视频的前3秒，截取第一帧，返回第一帧的图片(同步)
    public static byte[] extractFirstFrame(String bucket, String fileName, String localPath) throws IOException {
        downloadByName(bucket, fileName, localPath);
        // 本地视频文件路径
        String videoFilePath = localPath + "/" + fileName;
        // 提取视频文件名前缀
        String filePrefix = fileName.substring(0, fileName.lastIndexOf('.'));
        String outputImagePath = localPath + "/" + filePrefix + ".jpg";
        // 使用 ffmpeg 只读取前3秒并截取第一帧
        ProcessBuilder processBuilder = new ProcessBuilder(
                "ffmpeg", "-i", videoFilePath, "-t", "00:00:03", "-ss", "00:00:01", "-vframes", "1", outputImagePath);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        try {
            if (process.waitFor() != 0) {
                throw new IOException("ffmpeg process failed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("ffmpeg process was interrupted", e);
        }
        // 读取截取的第一帧图片
        byte[] imageBytes = Files.readAllBytes(Paths.get(outputImagePath));
        // 删除临时文件
        new File(videoFilePath).delete();
        //如果注释掉下面一行代码，那么localPath目录下的第一帧截图会被保留到本地
        //new File(outputImagePath).delete();

        return imageBytes;
    }
}
