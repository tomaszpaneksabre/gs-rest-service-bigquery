package com.example.restservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.Builder;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.http.HttpTransportOptions.DefaultHttpTransportFactory;

@RestController
public class GreetingController
{
    private static String projectId = "";
    private static String datasetId = "";
    private static String tableId = "greeting";

    private BigQuery bigQuery = initDefaultTransport();

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @GetMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name)
    {
        return new Greeting(counter.incrementAndGet(), String.format(template, name));
    }

    @GetMapping("/bigQuery")
    public String bigQuery()
    {
        Builder builder = InsertAllRequest.newBuilder(datasetId, tableId);
        for (int i = 0; i < 1; i++)
        {
            Map <String, Object> row = new HashMap <String, Object>();
            row.put("id", i);
            row.put("content", "" + i);
            builder.addRow(row);
        }
        return bigQuery.insertAll(builder.build()).toString();
    }

    private BigQuery initDefaultTransport()
    {
        return BigQueryOptions.newBuilder().setProjectId(projectId)
            .build().getService();
    }

    private BigQuery initApacheHttpTransport() 
    {
        com.google.cloud.http.HttpTransportOptions.Builder transportBuilder =
            HttpTransportOptions.newBuilder();
        HttpTransportFactory transportFactory = new DefaultHttpTransportFactory()
        {
            @Override
            public HttpTransport create()
            {
                return new ApacheHttpTransport();
            }
        };
        transportBuilder.setHttpTransportFactory(transportFactory);
        com.google.cloud.bigquery.BigQueryOptions.Builder optionsBuilder =
            BigQueryOptions.newBuilder();

        try
        {
            optionsBuilder
                .setCredentials(GoogleCredentials.getApplicationDefault(transportFactory));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return optionsBuilder.setProjectId(projectId).setTransportOptions(transportBuilder.build())
            .build().getService();
    }
}
