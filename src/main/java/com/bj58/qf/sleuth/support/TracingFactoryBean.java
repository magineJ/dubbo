package com.bj58.qf.sleuth.support;

import brave.Tracing;
import org.springframework.beans.factory.FactoryBean;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.okhttp3.OkHttpSender;

import java.util.concurrent.TimeUnit;

public class TracingFactoryBean implements FactoryBean<Tracing> {

    private Tracing tracing;

    private String applicationName;

    private String zipkinHost;

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public void setZipkinHost(String zipkinHost) {
        this.zipkinHost = zipkinHost;
    }

    public TracingFactoryBean(String applicationName, String zipkinHost) {
        this.applicationName = applicationName;
        this.zipkinHost = zipkinHost;

        synchronized (TracingFactoryBean.class) {
            if (tracing == null) {
                createInstance();
            }
        }
    }

    private void createInstance() {
        String endpoint = "http://" + zipkinHost + "/api/v2/spans";

        tracing = Tracing.newBuilder()
                .localServiceName(applicationName)
                .spanReporter(spanReporter(endpoint))
                .build();
    }

    private AsyncReporter<Span> spanReporter(String endpoint) {
        Sender sender = OkHttpSender
                .newBuilder()
                .endpoint(endpoint)
                .build();

        return AsyncReporter.builder(sender)
                .closeTimeout(500, TimeUnit.MILLISECONDS)
                .build(SpanBytesEncoder.JSON_V2);
    }

    @Override
    public Tracing getObject() throws Exception {
        if (tracing == null) {
            createInstance();
        }
        return tracing;
    }

    @Override
    public Class<?> getObjectType() {
        return Tracing.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
