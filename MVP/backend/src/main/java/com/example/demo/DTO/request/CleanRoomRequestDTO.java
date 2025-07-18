package com.example.demo.DTO.request;


public class CleanRoomRequestDTO {

    private String cleanRoomName;
    private String description;

    public CleanRoomRequestDTO(String cleanRoomName, String description){
        this.cleanRoomName = cleanRoomName;
        this.description = description;
    }

    public CleanRoomRequestDTO(String cleanRoomName){
        this.cleanRoomName = cleanRoomName;
    }

    public String getCleanRoomName() {
        return cleanRoomName;
    }
    public String getDescription() {
        return description;
    }
    public void setCleanRoomName(String cleanRoomName) {
        this.cleanRoomName = cleanRoomName;
    }
    public void setDescription(String description) {
        this.description = description;
    }

    
}
