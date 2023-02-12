package com.mtg.spark.loadData;

import com.mtg.spark.Utils.DataSetUtils;
import com.mtg.spark.constants.jdbcConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class loadStarCityGames {

    public void loadNonFoil(Dataset<Row> inputDs, String Date) {

        String dbConntectionURL = jdbcConstants.JDBCCONNECTIONURL;

        Dataset<Row> ckDs = inputDs.withColumn("Date", functions.lit(Date));

        Dataset<Row> reOrderedDs = new DataSetUtils().reOrderColumns(ckDs);

        Dataset<Row> sortedDs = reOrderedDs.orderBy("Name");

        //sortedDs.show(3);
        ScraperHistory.UpdateScraperHistory(Date,"Star City Games", "Non foil", sortedDs.count());

        sortedDs.write().format("jdbc").option("url", dbConntectionURL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "mtgcards.scg_non_foil")
                .option("user", "root")
                .option("password","1!April199#" )
                .mode("overwrite")
                .save();

    }

    public void loadFoil(Dataset<Row> inputDs, String Date) {

        String dbConntectionURL = jdbcConstants.JDBCCONNECTIONURL;

        Dataset<Row> ckDs = inputDs.withColumn("Date", functions.lit(Date));

        Dataset<Row> reOrderedDs = new DataSetUtils().reOrderColumns(ckDs);

        Dataset<Row> sortedDs = reOrderedDs.orderBy("Name");

        //sortedDs.show(3);
        ScraperHistory.UpdateScraperHistory(Date,"Star City Games", "Foil", sortedDs.count());

        sortedDs.write().format("jdbc").option("url", dbConntectionURL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "mtgcards.scg_foil")
                .option("user", "root")
                .option("password","1!April199#" )
                .mode("overwrite")
                .save();

    }
}
