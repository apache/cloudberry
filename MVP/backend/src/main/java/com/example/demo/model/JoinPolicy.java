package com.example.demo.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "join_policy")
public class JoinPolicy {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long joinPolicyId;

    @ManyToOne
    @JoinColumn(name = "user_clean_room")
    private UserCleanroom userCleanRoom;
    
}
