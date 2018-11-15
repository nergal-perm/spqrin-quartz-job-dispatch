package com.example.demo;

import org.springframework.stereotype.Component;
import org.apache.commons.lang3.RandomUtils;

@Component
public class DataImportService {


    public void importObjects(String provider, long l, String region) throws InterruptedException {
        // just emulate random work time
        Thread.sleep(RandomUtils.nextInt(500, 10000));
    }

    public void importEvents(String provider, long l, String region) throws InterruptedException {
        // just emulate random work time
        Thread.sleep(RandomUtils.nextInt(500, 10000));
    }
}
