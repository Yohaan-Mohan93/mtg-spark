package com.mtg.spark.loadData;

import com.mtg.spark.constants.jdbcConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ScraperHistory {

    public static  void UpdateScraperHistory(String Date, String website, String foil, Long  count) {
        try {
            Connection conn = DriverManager.getConnection(jdbcConstants.JDBCCONNECTIONURL, "root", jdbcConstants.MYSQLPASSWORD);

            String insertString = "INSERT INTO MTGCARDS.SCRAPER_HISTORY(DATE, WEBSITE, INSERT_COUNT, FOIL_NON_FOIL) VALUES(?,?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(insertString);
            preparedStatement.setString(1,Date);
            preparedStatement.setString(2,website);
            preparedStatement.setLong(3,count);
            preparedStatement.setString(4,foil);

            int row = preparedStatement.executeUpdate();

            System.out.println("Inserted into scraper_history: " + row + " rows");

        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        }
    }
}
