package org.elasticsearch.client.http;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;

/**
 *
 */
public class HttpClient extends AbstractClient {

    @SuppressWarnings("rawtypes")
    private final Map<GenericAction, HttpAction> actionMap;

    @SuppressWarnings("rawtypes")
    private final Map<Channel, HttpContext> httpContextMap;

    private ClientBootstrap bootstrap;

    private URL url;

    private HttpClient(Settings settings, ThreadPool threadPool, Headers headers, URL url) {
        super(settings, threadPool, headers);
        this.url = url;
        this.actionMap = new HashMap<>();
        this.httpContextMap = new HashMap<>();
        this.bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new HttpClientPipelineFactory());
        bootstrap.setOption("tcpNoDelay", true);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void close() {
        bootstrap.releaseExternalResources();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
    void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        HttpAction httpAction = actionMap.get(action);
        if (httpAction == null) {
            throw new IllegalStateException("failed to find http action [" + action + "] to execute");
        }
        try {
            HttpContext httpContext = new HttpContext(httpAction, listener, request);
            httpContext.httpRequest = httpAction.createHttpRequest(url, request);
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(url.getHost(), url.getPort()));
            future.awaitUninterruptibly();
            if (!future.isSuccess()) {
                bootstrap.releaseExternalResources();
                logger.error("can't connect to {}", url);
            } else {
                Channel channel = future.getChannel();
                httpContext.setChannel(channel);
                httpContextMap.put(channel, httpContext);
                channel.getConfig().setConnectTimeoutMillis(settings.getAsInt("http.client.timeout", 5000));
                httpAction.execute(httpContext, listener);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static class Builder {

        private HttpClient client;

        private Settings settings = Settings.EMPTY;

        private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        private URL url;

        private String host;

        private Integer port;

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder url(URL url) {
            this.url = url;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public  HttpClient build() {
            try {
                this.url = settings.get("url") != null ? new URL(settings.get("url")) : url;
                if (url == null) {
                    this.host = settings.get("host", "127.0.0.1");
                    this.port = settings.getAsInt("port", 9200);
                }
                if (url == null && host != null && port != null) {
                    url = new URL("http://" + host + ":" + port);
                }
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("malformed url: " + host + ":" + port);
            }
            if (url == null) {
                throw new IllegalArgumentException("no base URL given");
            }
            ThreadPool threadpool = new ThreadPool("http_client_pool");
            client = new HttpClient(settings, threadpool, Headers.EMPTY, url);
            ServiceLoader<HttpAction> httpActionServiceLoader = ServiceLoader.load(HttpAction.class, classLoader);
            for (HttpAction httpAction : httpActionServiceLoader) {
                httpAction.setSettings(settings);
                client.actionMap.put(httpAction.getActionInstance(), httpAction);
            }
            return client;
        }
    }

    private class HttpClientPipelineFactory implements ChannelPipelineFactory {

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpChunkAggregator(settings.getAsInt("http.client.maxchunksize", 10 * 1024 * 1024)));
            pipeline.addLast("inflater", new HttpContentDecompressor());
            pipeline.addLast("handler", new HttpResponseHandler<>());
            return pipeline;
        }
    }

    private class HttpResponseHandler<Request extends ActionRequest<Request>, Response extends ActionResponse> extends SimpleChannelUpstreamHandler {

        @SuppressWarnings("unchecked")
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = httpContextMap.get(ctx.getChannel());
            if (httpContext == null) {
                throw new IllegalStateException("no context for channel?");
            }
            try {
                if (e.getMessage() instanceof HttpResponse) {
                    HttpResponse httpResponse = (HttpResponse) e.getMessage();
                    HttpAction<Request, Response> action = httpContext.getHttpAction();
                    ActionListener<Response> listener = httpContext.getListener();
                    httpContext.setHttpResponse(httpResponse);
                    if (httpResponse.getContent().readable() && listener != null && action != null) {
                        listener.onResponse(action.createResponse(httpContext));
                    }
                }
            } finally {
                ctx.getChannel().close();
                httpContextMap.remove(ctx.getChannel());
            }
        }

        @SuppressWarnings("unchecked")
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            HttpContext<Request, Response> httpContext = httpContextMap.get(ctx.getChannel());
            try {
                if (httpContext != null && httpContext.getListener() != null) {
                    httpContext.getListener().onFailure(e.getCause());
                } else {
                    Throwable t = e.getCause();
                    logger.error(t.getMessage(), t);
                }
            } finally {
                ctx.getChannel().close();
                httpContextMap.remove(ctx.getChannel());
            }
        }
    }
}
