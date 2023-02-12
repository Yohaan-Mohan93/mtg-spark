package com.mtg.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import com.mtg.spark.loadData.*;

public class Application {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("com").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV To DB")
				.master("local")
				.getOrCreate();
		
		System.out.println("Website: " + args[0] + ", Foil/Non-Foil: " + args[1] + ", Date: " + args[2]);
		
		if(args[0].equals("ck") && args[1].equals("nf")) {
			Dataset<Row> ds = spark.read().format("csv")
					  .option("header", true)
					  .option("delimiter", "|")
					  .load("src/main/resources/CK_PRICES_" + args[2] + ".txt");
			
			loadCardKingdom loadCK = new loadCardKingdom();
			
			loadCK.loadNonFoil(ds,args[2]);
		}
		else if(args[0].equals("ck") && args[1].equals("f")) {
			Dataset<Row> ds = spark.read().format("csv")
					  .option("header", true)
					  .option("delimiter", "|")
					  .load("src/main/resources/CK_PRICES_" + args[2] + ".txt");
			
			loadCardKingdom loadCK = new loadCardKingdom();
			
			loadCK.loadFoil(ds,args[2]);
		}
		else if(args[0].equals("scg") && args[1].equals("nf")) {
			Dataset<Row> ds = spark.read().format("csv")
					.option("header", true)
					.option("delimiter", "|")
					.load("src/main/resources/SCG_PRICES_" + args[2] + ".txt");

			loadStarCityGames loadSCG = new loadStarCityGames();

			loadSCG.loadNonFoil(ds,args[2]);
		}
		else if(args[0].equals("scg") && args[1].equals("f")) {
			Dataset<Row> ds = spark.read().format("csv")
					.option("header", true)
					.option("delimiter", "|")
					.load("src/main/resources/SCG_PRICES_" + args[2] + ".txt");

			loadStarCityGames loadSCG = new loadStarCityGames();

			loadSCG.loadFoil(ds,args[2]);
		}
		
		
		
	}

}
