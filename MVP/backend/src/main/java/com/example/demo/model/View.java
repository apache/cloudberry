package com.example.demo.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.Cache;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
@Entity
@Table(name = "views")
public class View {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long viewId;

    @Column(name = "view_name")
    private String viewName;

    @ManyToOne
    @JoinColumn(name = "table_id")
    private UploadedTable table;

    @ManyToOne
    @JoinColumn(name = "user_cleanroom_id")
    private UserCleanroom userCleanroom;

    @Column(name = "sql_definition")
    private String sqlDefinition;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @ManyToOne
    @JoinColumn(name = "user_clean_room")
    private UserCleanroom userCleanRoom;




    
}
