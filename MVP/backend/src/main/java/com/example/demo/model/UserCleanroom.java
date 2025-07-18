package com.example.demo.model;

import java.util.List;

import org.hibernate.annotations.ManyToAny;

import com.example.demo.enums.CleanRoomUserEnum;
import com.example.demo.utils.CleanRoomUserEnumConverterUtil;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

@Entity
@Table(name = "user_cleanroom")
public class UserCleanroom {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long userCleanroomId;

    @OneToMany(mappedBy = "userCleanRoom", cascade = CascadeType.ALL)
    private List<ColumnRowPolicy> columnRowPolicies;

    @OneToMany(mappedBy = "userCleanRoom", cascade = CascadeType.ALL)
    private List<JoinPolicy> joinPolicies;

    @OneToMany(mappedBy = "userCleanRoom", cascade = CascadeType.ALL)
    private List<AggregationPolicy> aggregationPolicies;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private User user;

    @ManyToOne
    @JoinColumn(name = "clean_room")
    private CleanRoom cleanRoom;

    @OneToMany(mappedBy = "userCleanRoom", cascade = CascadeType.ALL)
    private List<View> views;

    // @OneToMany(mappedBy = "userCleanRoom", cascade = CascadeType.ALL)
    // private List<Schema> schemas;


    @Convert(converter = CleanRoomUserEnumConverterUtil.class)
    private CleanRoomUserEnum userStatus;

    public List<AggregationPolicy> getAggregationPolicies() {
        return aggregationPolicies;
    }
    public CleanRoom getCleanRoom() {
        return cleanRoom;
    }
    public List<ColumnRowPolicy> getColumnRowPolicies() {
        return columnRowPolicies;
    }
    public List<JoinPolicy> getJoinPolicies() {
        return joinPolicies;
    }
    public User getUser() {
        return user;
    }
    public Long getUserCleanroomId() {
        return userCleanroomId;
    }
    public CleanRoomUserEnum getUserStatus() {
        return userStatus;
    }
    public List<View> getViews() {
        return views;
    }
    public void setAggregationPolicies(List<AggregationPolicy> aggregationPolicies) {
        this.aggregationPolicies = aggregationPolicies;
    }
    public void setCleanRoom(CleanRoom cleanRoom) {
        this.cleanRoom = cleanRoom;
    }
    public void setColumnRowPolicies(List<ColumnRowPolicy> columnRowPolicies) {
        this.columnRowPolicies = columnRowPolicies;
    }
    public void setJoinPolicies(List<JoinPolicy> joinPolicies) {
        this.joinPolicies = joinPolicies;
    }
    public void setUser(User user) {
        this.user = user;
    }
    public void setUserCleanroomId(Long userCleanroomId) {
        this.userCleanroomId = userCleanroomId;
    }
    public void setUserStatus(CleanRoomUserEnum userStatus) {
        this.userStatus = userStatus;
    }
    public void setViews(List<View> views) {
        this.views = views;
    }

}
