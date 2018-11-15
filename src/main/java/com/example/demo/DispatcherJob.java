package com.example.demo;

import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.listeners.JobChainingJobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatcherJob implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(DispatcherJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        LOG.info("Dispatcher job started");

        JobKey dispatcher = new JobKey("SampleDispatcherJob", "DataImport");
        JobKey firstJob = new JobKey("SampleDataImportJob", "DataImport");
        JobKey secondJob = new JobKey("SampleChainedJob", "DataImport");

        JobChainingJobListener jobChain = new JobChainingJobListener("sampleChain");
        jobChain.addJobChainLink(dispatcher, firstJob);
        jobChain.addJobChainLink(firstJob, secondJob);

        try {
            jobExecutionContext.getScheduler().getListenerManager().addJobListener(jobChain, GroupMatcher.anyGroup());
        } catch (SchedulerException e) {
            LOG.error("Error while attaching JobListener", e);
        }

        LOG.info("Dispatcher job stopped");
    }
}
