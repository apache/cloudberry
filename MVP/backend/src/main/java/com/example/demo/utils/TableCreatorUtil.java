package com.example.demo.utils;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import javax.sql.DataSource;

import com.example.demo.enums.ErrorEnum;
import com.example.demo.exception.UploadException;
import com.opencsv.CSVReader;

public class TableCreatorUtil {
    public static void createTable(String csvPath, String tableName, DataSource dataSource) throws Exception{
        try(
            CSVReader reader = new CSVReader(new FileReader(csvPath));
            Connection conn = dataSource.getConnection();

        ){
            String[] headers = reader.readNext();
            String[] sampleRow = reader.readNext();

            if(headers == null || sampleRow == null){
                throw new UploadException(ErrorEnum.FILE_EMPTY);
            }

            StringBuilder sql = new StringBuilder("CREATE TABLE "+ tableName + " (");
            for (int i = 0; i < headers.length;i++){
                String colName = headers[i].trim().toLowerCase().replaceAll("[^a-z0-9_]", "_");
                String sample = sampleRow[i].trim();
                String type = inferType(sample);

                sql.append(colName).append(" ").append(type);
                if (i < headers.length - 1) sql.append(", ");
            }
            sql.append(");");

            System.out.println("创建表SQL：\n" + sql);
            Statement stmt = conn.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
            
        }

        

    }
    private static String inferType(String value){
        if (value.matches("^\\d+$")) return "INT";
        if (value.matches("^\\d+\\.\\d+$")) return "FLOAT";
        return "TEXT";
    }
    
}
