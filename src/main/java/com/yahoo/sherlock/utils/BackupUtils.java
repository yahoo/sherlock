package com.yahoo.sherlock.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * Back Util class for redis json dump.
 */
@Slf4j
public class BackupUtils {

    /* Gson object */
    private static Gson gson = new Gson();

    /**
     * Method to backup redis data into a file at {@link CLISettings#BACKUP_REDIS_DB_PATH}.
     * @throws IOException IO exception
     */
    public static void startBackup() throws IOException {
        JsonDumper jsonDumper = Store.getJsonDumper();
        JsonObject jsonObject = jsonDumper.getRawData();
        try (FileWriter file = new FileWriter(CLISettings.BACKUP_REDIS_DB_PATH)) {
            file.write(gson.toJson(jsonObject));
            file.close();
            log.info("Successfully wrote redis data to json file");
        }
    }

    /**
     * Method to read data from backup file.
     * @param path full system path to the file
     * @return redis data as json object
     * @throws FileNotFoundException exception if file not found
     */
    public static JsonObject getDataFromJsonFile(String path) throws FileNotFoundException {
        JsonReader reader = new JsonReader(new FileReader(path));
        JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
        return jsonObject;
    }
}
