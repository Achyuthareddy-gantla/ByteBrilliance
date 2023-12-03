package com.mycompany.app;

import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    // Goal 1
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split("\t");
        // Goal 1
         context.write(new Text(row[14]), new IntWritable(Integer.parseInt(row[6])));
    }

    // Goal 2
    // @Override
    // protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     try {
    //         String[] columns = value.toString().split("\t");

    //         if (columns.length >= 20) { 
    //             String dealSize = columns[19].trim();
    //             double sales = Double.parseDouble(columns[4].trim());
    //             context.write(new Text(dealSize), new IntWritable((int) sales));
    //         }
    //     } catch (Exception e) {
    //         // Handle parsing errors or other exceptions
    //     }
    // }

    // Goal 3
    // @Override
    // protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     try {
    //         String[] columns = value.toString().split("\t");

    //         if (columns.length >= 13) { 
    //             String customerName = columns[11].trim();
    //             int quantityOrdered = Integer.parseInt(columns[1].trim());
    //             context.write(new Text(customerName), new IntWritable(quantityOrdered));
    //         }
    //     } catch (Exception e) {
    //         // Handle parsing errors or other exceptions
    //     }
    // }

    // Goal 4
    // @Override
    // protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     try {
    //         String[] columns = value.toString().split("\t");

    //         if (columns.length >= 6) { 
    //             String orderDate = columns[5].trim();
    //             double sales = Double.parseDouble(columns[4].trim());

    //             String[] dateParts = orderDate.split("/");
    //             if (dateParts.length == 3) {
    //                 String yearMonth = dateParts[2] + "-" + dateParts[1]; // Assuming the format is dd/MM/yyyy
    //                 context.write(new Text(yearMonth), new IntWritable((int) sales));
    //             }
    //         }
    //     } catch (Exception e) {
    //         // Handle parsing errors or other exceptions
    //     }
    // } 

    // Goal 5
    // @Override
    // protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     try {
    //         String[] columns = value.toString().split("\t");

    //         if (columns.length >= 17) { 
    //             String country = columns[16].trim();
    //             int priceEach = Integer.parseInt(columns[2].trim());
    //             context.write(new Text(country), new IntWritable(priceEach));
    //         }
    //     } catch (Exception e) {
    //         // Handle parsing errors or other exceptions
    //     }
    // }
    

    // Goal 6
    // @Override
    // protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     try {
    //         String[] columns = value.toString().split("\t");

    //         if (columns.length >= 5) { 
    //             String productLine = columns[8].trim();
    //             int sales = Integer.parseInt(columns[4].trim());
    //             context.write(new Text(productLine), new IntWritable(sales));
    //         }
    //     } catch (Exception e) {
    //         // Handle parsing errors or other exceptions
    //     }
    // }
}