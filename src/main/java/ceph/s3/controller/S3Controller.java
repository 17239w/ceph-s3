package ceph.s3.controller;


import ceph.s3.file.util.AwzS3Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/awz/s3")
public class S3Controller {

    @PostMapping("/createBucket")
    public ResponseEntity<String> createBucket(@RequestParam("bucket") String bucket) {
        try {
            boolean created = AwzS3Util.createBucket(bucket);
            if (created) {
                return new ResponseEntity<>("桶创建成功!", HttpStatus.OK);
            } else {
                return new ResponseEntity<>("桶创建失败!", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("桶创建失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/deleteBucket")
    public ResponseEntity<String> deleteBucket(@RequestParam("bucket") String bucket) {
        try {
            boolean deleted = AwzS3Util.deleteBucket(bucket);
            if (deleted) {
                return new ResponseEntity<>("桶删除成功!", HttpStatus.OK);
            } else {
                return new ResponseEntity<>("桶删除失败!", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("桶删除失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "/uploadObjectByBlock")
    public ResponseEntity uploadObjectByBlock(@RequestParam("file") MultipartFile file, @RequestParam("bucket") String bucket) {
        try {
            boolean uploadSuccess = AwzS3Util.uploadByBlock(file, bucket);
            if (uploadSuccess) {
                return new ResponseEntity<>("上传成功!", HttpStatus.OK);
            } else {
                return new ResponseEntity<>("上传失败!", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("上传失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "/uploadOneBlock")
    public ResponseEntity uploadOneBlock(@RequestParam("file") MultipartFile file,
                                         @RequestParam("bucket") String bucket,
                                         @RequestParam("position") int position,
                                         @RequestParam("blockSize") long blockSize) {
        try {
            String uploadedKey = AwzS3Util.uploadOneBlock(file, position, blockSize, bucket);
            if (uploadedKey != null) {
                return new ResponseEntity<>("上传成功，Key：" + uploadedKey, HttpStatus.OK);
            } else {
                return new ResponseEntity<>("上传失败!", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("上传失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/deleteFile")
    public ResponseEntity<String> deleteFile(@RequestParam("bucket") String bucket, @RequestParam("fileName") String fileName) {
        try {
            boolean deleteSuccess = AwzS3Util.deleteFile(bucket, fileName);
            if (deleteSuccess) {
                return new ResponseEntity<>("文件删除成功!", HttpStatus.OK);
            } else {
                return new ResponseEntity<>("文件删除失败!", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("文件删除失败!", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping(value = "/downloadObject")
    public ResponseEntity downloadObject(String bucket, String fileName) {
        ResponseEntity<byte[]> download = null;
        try {
            download = AwzS3Util.downloadByName(bucket, fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return download;
    }

    @PostMapping(value = "/getFileInfo")
    public ObjectMetadata getFileInfo(String bucket, String fileName) {
        return AwzS3Util.getFileInfo(bucket, fileName);
    }

    @PostMapping(value = "/decompressAndUpload")
    public ResponseEntity<String> decompressAndUploadFile(
            @RequestParam String sourceBucket,
            @RequestParam String sourceKey,
            @RequestParam String targetBucket) {
        try {
            boolean result = AwzS3Util.decompressAndUpload(sourceBucket, sourceKey, targetBucket);
            if (result) {
                return ResponseEntity.ok("文件成功解压并上传到目标桶！");
            } else {
                return ResponseEntity.internalServerError().body("文件解压并上传失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("服务器内部错误：" + e.getMessage());
        }
    }

}
