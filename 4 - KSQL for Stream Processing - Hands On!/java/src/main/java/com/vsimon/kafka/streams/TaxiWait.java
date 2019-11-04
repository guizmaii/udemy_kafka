package com.vsimon.kafka.streams;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "taxi_wait", description = "Return expected wait time in minutes")

public class TaxiWait {


    @Udf(description = "Given weather and distance return expected wait time in minutes")
    public double taxi_wait(final String weather_description,  final double dist) {

        double ret = -1;
        double weather_factor = 1;

        switch(weather_description) 
        { 
            case "light rain": 
                weather_factor = 2;
                break; 

            case "heavy rain": 
                weather_factor = 4;
                break; 

            case "fog": 
                weather_factor = 6;
                break; 

            case "haze": 
                weather_factor = 1.5;
                break; 

            case "SUNNY": 
                weather_factor = 1;
                break;                 
        }

        return dist * weather_factor / 50.0;
    }
}





