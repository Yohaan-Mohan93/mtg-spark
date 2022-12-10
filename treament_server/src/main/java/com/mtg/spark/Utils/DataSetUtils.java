package com.mtg.spark.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class DataSetUtils {

    public Dataset<Row>  reOrderColumns(Dataset<Row> inputDataset){
        Dataset<Row> results;

        String[] columns = inputDataset.columns();
        String[] result = new String[columns.length];
        System.arraycopy(columns,1,result,2,columns.length - 2);
        result[0] = columns[0];
        result[1] = columns[columns.length - 1];

        results = inputDataset.select(result[0], Arrays.copyOfRange(result,1,result.length));

        return results;
    }
}
