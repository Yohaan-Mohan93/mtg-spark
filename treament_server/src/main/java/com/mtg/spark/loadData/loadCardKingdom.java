package com.mtg.spark.loadData;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.mtg.spark.constants.*;

public class loadCardKingdom {
	
	public void loadNonFoil(Dataset<Row> inputDs) {

		String dbConntectionURL = jdbcConstants.JDBCCONNECTIONURL;
		
		Dataset<Row> sortedDs = inputDs.orderBy("Name");
		
		sortedDs.show(3);
		
		sortedDs.write().format("jdbc").option("url", dbConntectionURL)
									   .option("driver", "com.mysql.cj.jdbc.Driver")
									   .option("dbtable", "mtgcards.card_kingdom")
									   .option("user", "root")
									   .option("password","1!April199#" )
									   .mode("overwrite")
									   .save();

	}
	
	public void loadFoil(Dataset<Row> inputDs) {

		String dbConntectionURL = jdbcConstants.JDBCCONNECTIONURL;
		
		Dataset<Row> sortedDs = inputDs.orderBy("Name");
		
		sortedDs.show(3);
		
		sortedDs.write().format("jdbc").option("url", dbConntectionURL)
									   .option("driver", "com.mysql.cj.jdbc.Driver")
									   .option("dbtable", "mtgcards.card_kingdom_foil")
									   .option("user", "root")
									   .option("password","1!April199#" )
									   .mode("overwrite")
									   .save();

	}
}
