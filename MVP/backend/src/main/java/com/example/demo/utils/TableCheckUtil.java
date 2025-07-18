package com.example.demo.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

public class TableCheckUtil {
    public static boolean tableExists(String tableName, DataSource dataSource) throws SQLException{
        String sql = "SELECT EXISTS (" +
                     "SELECT FROM information_schema.tables " +
                     "WHERE table_schema = 'public' AND table_name = ?" +
                     ")";
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);)
        {
            stmt.setString(1, tableName.toLowerCase());
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getBoolean(1);
            }
        }
        return false;
    }
}
