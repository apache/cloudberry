package com.example.demo.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "column_row_policy")
public class ColumnRowPolicy extends Policy{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long columnRowPolicyId;

    @ManyToOne
    @JoinColumn(name = "user_clean_room")
    private UserCleanroom userCleanRoom;
    
}
