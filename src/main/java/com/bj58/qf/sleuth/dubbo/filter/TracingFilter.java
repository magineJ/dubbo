package com.bj58.qf.sleuth.dubbo.filter;

import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.json.JSON;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.rpc.*;
import com.alibaba.dubbo.rpc.protocol.dubbo.FutureAdapter;
import com.alibaba.dubbo.rpc.support.RpcUtils;
import com.bj58.qf.sleuth.support.SleuthSpringContext;
import zipkin2.Endpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Future;

@Activate(group = {Constants.PROVIDER, Constants.CONSUMER})
public final class TracingFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(TracingFilter.class);

    private Tracing tracing;

    private Tracer tracer;

    private TraceContext.Extractor<Map<String, String>> extractor;

    private TraceContext.Injector<Map<String, String>> injector;

    public TracingFilter() {
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        tracing = (Tracing) SleuthSpringContext.getBean("tracing");
        tracer = tracing.tracer();
        extractor = tracing.propagation().extractor(GETTER);
        injector = tracing.propagation().injector(SETTER);

        RpcContext rpcContext = RpcContext.getContext();
        Kind kind = rpcContext.isProviderSide() ? Kind.SERVER : Kind.CLIENT;
        final Span span;
        if (kind.equals(Kind.CLIENT)) {
            span = tracer.nextSpan();
            injector.inject(span.context(), invocation.getAttachments());
        } else {
            TraceContextOrSamplingFlags extracted = extractor.extract(invocation.getAttachments());
            span = extracted.context() != null
                    ? tracer.joinSpan(extracted.context())
                    : tracer.nextSpan(extracted);
        }

        if (!span.isNoop()) {
            span.kind(kind).start();
            String service = invoker.getInterface().getSimpleName();
            String method = RpcUtils.getMethodName(invocation);

            span.kind(kind);
            span.name(service + "/" + method);

            InetSocketAddress remoteAddress = rpcContext.getRemoteAddress();
            Endpoint.Builder remoteEndpoint = Endpoint.newBuilder().port(remoteAddress.getPort());
            if (!remoteEndpoint.parseIp(remoteAddress.getAddress())) {
                remoteEndpoint.parseIp(remoteAddress.getHostName());
            }
            span.remoteEndpoint(remoteEndpoint.build());
        }

        boolean isOneway = false, deferFinish = false;
        try (Tracer.SpanInScope scope = tracer.withSpanInScope(span)) {
            collectArguments(invocation, span, kind);

            Result result = invoker.invoke(invocation);

            if (result.hasException()) {
                onError(result.getException(), span);
            }

            isOneway = RpcUtils.isOneway(invoker.getUrl(), invocation);
            // the case on async client invocation
            Future<Object> future = rpcContext.getFuture();

            if (future instanceof FutureAdapter) {
                deferFinish = true;
                ((FutureAdapter) future).getFuture().setCallback(new FinishSpanCallback(span));
            }
            return result;
        } catch (Error | RuntimeException e) {
            onError(e, span);
            throw e;
        } finally {
            if (isOneway) {
                span.flush();
            } else if (!deferFinish) {
                span.finish();
            }
        }
    }

    static void onError(Throwable error, Span span) {
        span.error(error);
        if (error instanceof RpcException) {
            span.tag("dubbo.error_msg", RpcExceptionEnum.getMsgByCode(((RpcException) error).getCode()));
        }
    }

    static void collectArguments(Invocation invocation, Span span, Kind kind) {
        if (kind == Kind.CLIENT) {
            StringBuilder fqcn = new StringBuilder();
            Object[] args = invocation.getArguments();
            if (args != null && args.length > 0) {
                try {
                    fqcn.append(JSON.json(args));
                } catch (IOException e) {
                    log.warn(e.getMessage(), e);
                }
            }

            span.tag("args", fqcn.toString());
        }
    }

    static final Propagation.Getter<Map<String, String>, String> GETTER =
            new Propagation.Getter<Map<String, String>, String>() {
                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier.get(key);
                }

                @Override
                public String toString() {
                    return "Map::get";
                }
            };

    static final Propagation.Setter<Map<String, String>, String> SETTER =
            new Propagation.Setter<Map<String, String>, String>() {
                @Override
                public void put(Map<String, String> carrier, String key, String value) {
                    carrier.put(key, value);
                }

                @Override
                public String toString() {
                    return "Map::set";
                }
            };

    static final class FinishSpanCallback implements ResponseCallback {
        final Span span;

        FinishSpanCallback(Span span) {
            this.span = span;
        }

        @Override
        public void done(Object response) {
            span.finish();
        }

        @Override
        public void caught(Throwable exception) {
            onError(exception, span);
            span.finish();
        }
    }

    private enum RpcExceptionEnum {
        UNKNOWN_EXCEPTION(0, "unknown exception"),
        NETWORK_EXCEPTION(1, "network exception"),
        TIMEOUT_EXCEPTION(2, "timeout exception"),
        BIZ_EXCEPTION(3, "biz exception"),
        FORBIDDEN_EXCEPTION(4, "forbidden exception"),
        SERIALIZATION_EXCEPTION(5, "serialization exception"),;

        private int code;

        private String msg;

        RpcExceptionEnum(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }

        public static String getMsgByCode(int code) {
            for (RpcExceptionEnum error : RpcExceptionEnum.values()) {
                if (code == error.code) {
                    return error.msg;
                }
            }
            return null;
        }
    }

}
