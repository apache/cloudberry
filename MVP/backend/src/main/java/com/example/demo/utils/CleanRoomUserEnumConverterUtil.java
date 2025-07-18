package com.example.demo.utils;

import com.example.demo.enums.CleanRoomUserEnum;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
@Converter(autoApply = true)
public class CleanRoomUserEnumConverterUtil implements AttributeConverter<CleanRoomUserEnum, Integer>{

    

    @Override
    public Integer convertToDatabaseColumn(CleanRoomUserEnum message) {
        return message == null ? null: message.getCode();
    }

    @Override
    public CleanRoomUserEnum convertToEntityAttribute(Integer code) {
        if (code == null){
            return null;
        }else{
            for (CleanRoomUserEnum e: CleanRoomUserEnum.values()){
                if(e.getCode() == code){
                    return e;
                }
            }
        }
        throw new IllegalArgumentException("Unknows database value of CleanRoomUserEnum: " + code);
    }
        
    
    
}
