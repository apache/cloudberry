package com.example.demo.service.impl;

import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.example.demo.DTO.UserDetailsDTO;
import com.example.demo.config.UploadProperties;
import com.example.demo.enums.ErrorEnum;
import com.example.demo.exception.UploadException;
import com.example.demo.model.UploadedTable;
import com.example.demo.repository.UploadedTableRepository;
import com.example.demo.repository.UserRepository;
import com.example.demo.service.UploadService;
import com.example.demo.utils.GploadYamlGeneratorUtils;
import com.example.demo.utils.TableCheckUtil;
import com.example.demo.utils.TableCreatorUtil;

import jakarta.transaction.Transactional;

@Service
public class UploadServiceImpl implements UploadService{
    @Autowired
    private UploadProperties uploadProperties;

    @Autowired
    private DataSource dataSource;
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private UploadedTableRepository uploadedTableRepository;

    @Override
    @Transactional
    public void processUpload(MultipartFile uploadFile) throws Exception{
        // if(errorLimit < 0){
        //     throw new UploadException(ErrorEnum.NEGATIVE_ERROR_LIMIT);
        // }
        
        UserDetailsDTO user = (UserDetailsDTO)SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        Long userId = user.getUserId();
        String corpName = user.getCorpName();

        //save csv file
        String dir = uploadProperties.getDir() + corpName + "/";
        new File(dir).mkdirs();
        String savedPath = dir + uploadFile.getOriginalFilename();
        uploadFile.transferTo(new File(savedPath));

        
        String originalFileName = uploadFile.getOriginalFilename();
        if (originalFileName == null || originalFileName.isEmpty()) {
            throw new UploadException(ErrorEnum.FILE_NAME_MISSING);
        }
        
        String lowerCaseName = originalFileName.toLowerCase();
        if (!lowerCaseName.endsWith(".csv") && !lowerCaseName.endsWith(".json")) {
            throw new UploadException(ErrorEnum.FILE_TYPE_ERROR);
        }
        
        String baseName = originalFileName.contains(".")
        ? originalFileName.substring(0, originalFileName.lastIndexOf("."))
        : originalFileName;
        String tableName = "upload_" + userId + "_" +baseName.replaceAll("[^a-zA-Z0-9_]", "_");
        String yamlPath = dir + "gpload_config.yaml";

        //create database table to store loaded data from csv
        if(!TableCheckUtil.tableExists(tableName, dataSource)){
            TableCreatorUtil.createTable(savedPath, tableName, dataSource);
        }

        try{
            UploadedTable uploadedTable = new UploadedTable();
            uploadedTable.setOriginalFileName(baseName);
            uploadedTable.setTableName(tableName);
            uploadedTable.setUser(userRepository.findUserByEmail(user.getEmail()));
            uploadedTable.setUploadTime(LocalDateTime.now());
            uploadedTableRepository.save(uploadedTable);
        }catch(UploadException e){
            throw new UploadException(ErrorEnum.FILE_UPLOAD_FAIL);
        }catch(RuntimeException e){
            throw new RuntimeException(e.toString());
        }
        

        //generate gpload YAML document
        GploadYamlGeneratorUtils.generateYaml(savedPath, tableName, yamlPath);

        //execute gpload command
        String gploadPath = uploadProperties.getGploadPath();
        ProcessBuilder pb = new ProcessBuilder(gploadPath, "-f", yamlPath);

        pb.inheritIO();
        Process process = pb.start();
        String result = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if(exitCode != 0){
            throw new UploadException(ErrorEnum.FILE_UPLOAD_FAIL);
        }
        
    }
    
}
