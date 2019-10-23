package com.soterocra.javamongoimport;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.IOUtils;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {


    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("");
        MongoCollection<Document> collection = database.getCollection("");
        String fileName = "";

        String regexSeparator = "\\|";

        FileInputStream fis = null;
        try {
            fis = new FileInputStream("src/main/resources/" + fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String data = null;

        try {
            data = IOUtils.toString(fis, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

//        Map<String, Object> headers = new TreeMap<>();

        assert data != null;
        List<String> lines = new ArrayList<>(Arrays.asList(data.split(System.lineSeparator())));

        List<String> headers = new ArrayList<>(Arrays.asList(lines.get(0).split(regexSeparator)));
        lines.remove(0);

        lines.parallelStream().forEach(l -> {
            List<String> tmpList = new ArrayList<>(Arrays.asList(l.split(regexSeparator)));
            Document tmpDocument = new Document();

            for (int i = 0; i < tmpList.size(); i++) {
                tmpDocument.put(headers.get(i), tmpList.get(i));
            }

            collection.insertOne(tmpDocument);
        });

        data = null;


    }


}
