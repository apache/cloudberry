package com.example.demo.utils;

import java.io.FileWriter;
import java.io.IOException;

public class GploadYamlGeneratorUtils {
    public static void generateYaml(String csvPath, String tableName, String yamlPath)throws IOException{
        String yamlContent = String.format("""
VERSION: 1.0.0.1
DATABASE: template1
USER: jiahe
HOST: localhost
PORT: 5432
GPLOAD:
  INPUT:
    SOURCE:
      LOCAL_HOSTNAME:
        - localhost
      FILE:
        - %s
    FORMAT: csv
    HEADER: true
    DELIMITER: ','
    NULL_AS: ''
  OUTPUT:
    - TABLE: %s
      MODE: INSERT
  
""",csvPath, tableName);
        try(FileWriter writer = new FileWriter(yamlPath)){
            
            writer.write(yamlContent);
        }

    }

    
    
}
