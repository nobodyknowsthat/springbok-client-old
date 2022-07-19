package com.anonymous.test.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-26 4:32 PM
 **/
public class SpringbokClient {

    private String rootUrl;  // format like "http://localhost:8001"

    private ObjectMapper objectMapper = new ObjectMapper();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public SpringbokClient(String rootUrl) {
        this.rootUrl = rootUrl;
    }

    public String insert(List<TrajectoryPoint> pointList) throws Exception {


        String stringToSend = objectMapper.writeValueAsString(pointList);

        // set and send request
        String urlString = rootUrl + "/insertion";
        logger.info("url string: " + urlString);
        URL url = new URL(urlString);
        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(connection.getOutputStream());
        outputStreamWriter.write(stringToSend);
        outputStreamWriter.close();

        // get response
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        connection.getInputStream()));
        StringBuilder stringBuilder = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            stringBuilder.append(inputLine);
        }
        in.close();

        return stringBuilder.toString();
    }

    public String asyncInsert(List<TrajectoryPoint> pointList) throws Exception {
        String stringToSend = objectMapper.writeValueAsString(pointList);

        // set and send request
        String urlString = rootUrl + "/asyncinsertion";
        logger.info("url string: " + urlString);

        URL url = new URL(urlString);
        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(connection.getOutputStream());
        outputStreamWriter.write(stringToSend);
        outputStreamWriter.close();

        // get response
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        connection.getInputStream()));
        StringBuilder stringBuilder = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            stringBuilder.append(inputLine);
        }
        in.close();

        return stringBuilder.toString();
    }

    public String idTemporalQuery(IdTemporalQueryPredicate predicate) throws Exception {
        String predicateJson = objectMapper.writeValueAsString(predicate);

        String urlString = rootUrl + "/idtemporalquery";
        logger.info("url string: " + urlString);

        URL url = new URL(urlString);
        String stringToSend = IOUtils.toString(predicateJson.getBytes(StandardCharsets.UTF_8), "UTF-8");

        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        OutputStreamWriter out = new OutputStreamWriter(
                connection.getOutputStream());
        out.write(stringToSend);
        out.close();


        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        connection.getInputStream()));

        StringBuilder stringBuilder = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            stringBuilder.append(inputLine);
        }
        in.close();
        return stringBuilder.toString();
    }

    public String spatialTemporalQuery(SpatialTemporalRangeQueryPredicate predicate) throws Exception {
        String predicateJson = objectMapper.writeValueAsString(predicate);

        String urlString = rootUrl + "/spatialtemporalquery";
        logger.info("url string: " + urlString);

        URL url = new URL(urlString);
        String stringToSend = IOUtils.toString(predicateJson.getBytes(StandardCharsets.UTF_8), "UTF-8");

        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        OutputStreamWriter out = new OutputStreamWriter(
                connection.getOutputStream());
        out.write(stringToSend);
        out.close();


        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        connection.getInputStream()));

        StringBuilder stringBuilder = new StringBuilder();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            stringBuilder.append(inputLine);
        }
        in.close();
        return stringBuilder.toString();
    }

}
