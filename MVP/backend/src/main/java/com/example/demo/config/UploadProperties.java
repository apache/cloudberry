package com.example.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="upload")
public class UploadProperties {
    private String dir;
    private String gploadPath;

    public String getDir() {
        return dir;
    }
    public void setDir(String dir) {
        this.dir = dir;
    }
    public String getGploadPath() {
        return gploadPath;
    }
    public void setGploadPath(String gploadPath) {
        this.gploadPath = gploadPath;
    }

    
}
