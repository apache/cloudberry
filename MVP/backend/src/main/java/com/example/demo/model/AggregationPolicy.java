package com.example.demo.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "aggregation_policy")
public class AggregationPolicy {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long aggregationId;

    @ManyToOne
    @JoinColumn(name = "user_clean_room")
    private UserCleanroom userCleanRoom;
    
}
