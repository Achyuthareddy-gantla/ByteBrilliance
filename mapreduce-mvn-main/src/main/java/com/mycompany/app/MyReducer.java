package com.mycompany.app;

import java.io.IOException;

//import org.apache.hadoop.io.DoubleWritable;
// import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    // Goal 1
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        // Goal 1
        int sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        int average = sum / count;
        context.write(key, new IntWritable(average));      
    }
    
    // Goal 2
    // @Override
    // protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    //         throws IOException, InterruptedException {
    //     try {
    //         int count = 0;
    //         int totalSales = 0;

    //         for (IntWritable value : values) {
    //             totalSales += value.get();
    //             count++;
    //         }

    //         if (count > 0) {
    //             int averageSales = totalSales / count;
    //             context.write(key, new IntWritable(averageSales));
    //         }
    //     } catch (Exception e) {
    //         // Handle exceptions
    //     }
    // }


    // Goal 3
    // @Override
    // protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    //         throws IOException, InterruptedException {
    //     try {
    //         int totalQuantity = 0;

    //         for (IntWritable value : values) {
    //             totalQuantity += value.get();
    //         }

    //         context.write(key, new IntWritable(totalQuantity));
    //     } catch (Exception e) {
    //         // Handle exceptions
    //     }
    // }

    // Goal 4
    // @Override
    // protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    //         throws IOException, InterruptedException {
    //     try {
    //         int totalSales = 0;

    //         for (IntWritable value : values) {
    //             totalSales += value.get();
    //         }

    //         context.write(key, new IntWritable(totalSales));
    //     } catch (Exception e) {
    //         // Handle exceptions
    //     }
    // }

    // Goal 5
    // @Override
    // protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    //         throws IOException, InterruptedException {
    //     try {
    //         int totalPrices = 0;
    //         int count = 0;

    //         for (IntWritable value : values) {
    //             totalPrices += value.get();
    //             count++;
    //         }

    //         if (count > 0) {
    //             int averagePrice = totalPrices / count;
    //             context.write(key, new IntWritable(averagePrice));
    //         }
    //     } catch (Exception e) {
    //         // Handle exceptions
    //     }
    // }


    // Goal 6
    // @Override
    // protected void reduce(Text key, Iterable<IntWritable> values, Context context)
    //         throws IOException, InterruptedException {
    //     try {
    //         int totalSales = 0;

    //         for (IntWritable value : values) {
    //             totalSales += value.get();
    //         }

    //         context.write(key, new IntWritable(totalSales));
    //     } catch (Exception e) {
    //         // Handle exceptions
    //     }
    // }
}