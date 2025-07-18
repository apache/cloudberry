package com.example.demo.service;

import java.io.File;
import java.io.IOException;

import org.springframework.web.multipart.MultipartFile;

public interface UploadService {

    void processUpload(MultipartFile uploadFile) throws Exception;
}
