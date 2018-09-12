package com.bj58.qf.sleuth.support;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SleuthSpringContext implements ApplicationContextAware {

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    public static ApplicationContext getContext() {
        return context;
    }

    public static <T> T getBean(Class<T> type) throws BeansException {
        return context.getBean(type);
    }

    public static Object getBean(String name) throws BeansException {
        return context.getBean(name);
    }

}
