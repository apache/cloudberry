package com.example.demo.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.example.demo.exception.JsonConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
@Converter(autoApply = true)
public class ListToJsonConverterUtil implements AttributeConverter<List<String>, String>{

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(List<String> attribute) {
        if (attribute == null || attribute.isEmpty()){
            return "[]";
        }else{
            try{
                return objectMapper.writeValueAsString(attribute);
            }catch(JsonProcessingException e){
                throw new JsonConversionException("Error converting list to JSON", e);
            }
        }
        
    }

    @Override
    public List<String> convertToEntityAttribute(String dbData) {
        if(dbData == null || dbData.isEmpty()){
            return new ArrayList<>();
        }else{
            try{
                return objectMapper.readValue(dbData, List.class);
            }catch(IOException e){
                throw new JsonConversionException("Error reading JSON to list", e);
            }
        }
    }

    
}
