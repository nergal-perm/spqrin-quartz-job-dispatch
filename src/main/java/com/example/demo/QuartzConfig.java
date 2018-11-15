package com.example.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

@Configuration
public class QuartzConfig {

    @Value("${demo.quartz-property-file}")
    private String quartzPropertyFile;

    @Bean
    public SpringBeanJobFactory springBeanJobFactory() {
        return new SpringBeanJobFactory();
    }

    @Bean
    public SchedulerFactoryBean scheduler() {
        SchedulerFactoryBean schedulerFactory = new SchedulerFactoryBean();

        schedulerFactory.setOverwriteExistingJobs(true);
        schedulerFactory.setConfigLocation(new FileSystemResource(quartzPropertyFile));
        schedulerFactory.setJobFactory(springBeanJobFactory());

        return schedulerFactory;
    }

}
