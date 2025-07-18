package com.example.demo.DTO.request;

import java.util.List;

public class RowColumnPolicyRequestDTO {
    private List<String> rows;
    private List<String> columns;
    private List<Long> tables;
    private Long cleanRoomId;

    public RowColumnPolicyRequestDTO(List<String> rows, List<String>cokumns, List<Long> tables, Long cleanRoomId){

        this.columns = columns;
        this.rows = rows;
        this.cleanRoomId = cleanRoomId;
        this.tables = tables;
    }
    public List<String> getColumns() {
        return columns;
    }
    public List<String> getRows() {
        return rows;
    }
    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
    public void setRows(List<String> rows) {
        this.rows = rows;
    }
    public Long getCleanRoomId() {
        return cleanRoomId;
    }
    public List<Long> getTables() {
        return tables;
    }
    public void setCleanRoomId(Long cleanRoomId) {
        this.cleanRoomId = cleanRoomId;
    }
    public void setTables(List<Long> tables) {
        this.tables = tables;
    }

    
}
