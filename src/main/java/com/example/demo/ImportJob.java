package com.example.demo;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ImportJob implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(ImportJob.class);

    @Value("${import-objects.thread.count}")
    private int threadCount;

    @Autowired
    private DataImportService dataImportService;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOG.info("Started ImportData job");
        try {
            importObjectsMultithreaded();
            importEventsSingleThreaded();
        } catch (InterruptedException e) {
            LOG.error("Interrupted thread execution", e);
        }
        LOG.info("Finished ImportData job");
    }

    private void importEventsSingleThreaded() throws InterruptedException {
        List<Map<String, Object>> instancesParameters = getInstanceParameters();

        List<Callable<Boolean>> objectImportTasks = new ArrayList<>();
        for (Map<String, Object> parameters : instancesParameters) {
            String region = parameters.get("region").toString();
            objectImportTasks.add(new EventImportCallable(dataImportService, region));
        }
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.invokeAll(objectImportTasks);
    }

    private void importObjectsMultithreaded() throws InterruptedException {
        List<Map<String, Object>> instancesParameters = getInstanceParameters();

        List<Callable<Boolean>> objectImportTasks = new ArrayList<>();
        for (Map<String, Object> parameters : instancesParameters) {
            String region = parameters.get("region").toString();
            objectImportTasks.add(new ObjectImportCallable(dataImportService, region));
        }
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        executorService.invokeAll(objectImportTasks);

    }

    private List<Map<String, Object>> getInstanceParameters() {
        List<Map<String, Object>> result = new ArrayList<>();
        result.add(new HashMap<String, Object>() {{
            put("region", "msk");
        }});
        result.add(new HashMap<String, Object>() {{
            put("region", "spb");
        }});
        result.add(new HashMap<String, Object>() {{
            put("region", "prm");
        }});
        return result;
    }
}


class AbstractImportCallable implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(ImportJob.class);
    private String jobName;

    public AbstractImportCallable(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public Boolean call() throws Exception {
        LOG.info("{} started", jobName);
        StopWatch watch = new StopWatch();
        watch.start();
        try {
            runSpecificImportJob();
        } catch (Exception e) {
            LOG.error(jobName + " failed", e);
            throw new JobExecutionException(jobName + " failed", e);
        } finally {
            watch.stop();
            LOG.info("{} completed in {} millis", jobName, watch.getLastTaskTimeMillis());
        }
        return true;
    }

    protected void runSpecificImportJob() throws InterruptedException {
        // should be overriden in child classes
    }
}

class ObjectImportCallable extends AbstractImportCallable {
    private DataImportService dataImportService;
    private String region;

    public ObjectImportCallable(DataImportService dataImportService, String region) {
        super("Multithreaded object import job for " + region + " region");
        this.dataImportService = dataImportService;
        this.region = region;
    }

    @Override
    protected void runSpecificImportJob() throws InterruptedException {
        dataImportService.importObjects("provider", 1L, region);
    }
}

class EventImportCallable extends AbstractImportCallable {
    private DataImportService dataImportService;
    private String region;

    public EventImportCallable(DataImportService dataImportService, String region) {
        super("Singlethreaded event import job for " + region + " region");
        this.dataImportService = dataImportService;
        this.region = region;
    }

    @Override
    protected void runSpecificImportJob() throws InterruptedException {
        dataImportService.importEvents("provider", 1L, region);
    }
}