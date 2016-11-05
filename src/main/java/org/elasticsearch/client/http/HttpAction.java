package org.elasticsearch.client.http;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

/**
 * @param <R> the request type
 * @param <T> the response type
 */
public abstract class HttpAction<R extends ActionRequest<R>, T extends ActionResponse> {

    protected final ESLogger logger = ESLoggerFactory.getLogger(getClass().getName());
    protected Settings settings;

    protected void setSettings(Settings settings) {
        this.settings = settings;
    }

    public abstract GenericAction<R, T> getActionInstance();

    public final ActionFuture<T> execute(HttpContext<R, T> httpContext) {
        PlainActionFuture<T> future = newFuture();
        execute(httpContext, future);
        return future;
    }

    public final void execute(HttpContext<R, T> httpContext, ActionListener<T> listener) {
        ActionRequestValidationException validationException = httpContext.request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        httpContext.listener = listener;
        httpContext.millis = System.currentTimeMillis();
        try {
            doExecute(httpContext);
        } catch (Exception e) {
            logger.error("exception during http action execution", e);
            listener.onFailure(e);
        }
    }

    protected HttpRequest newGetRequest(URL url, String path) {
        return newGetRequest(url, path, null);
    }

    protected HttpRequest newGetRequest(URL url, String path, CharSequence content) {
        return newRequest(HttpMethod.GET, url, path, content);
    }

    protected HttpRequest newPostRequest(URL url, String path, CharSequence content) {
        return newRequest(HttpMethod.POST, url, path, content);
    }

    protected HttpRequest newPutRequest(URL url, String path, CharSequence content) {
        return newRequest(HttpMethod.PUT, url, path, content);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, CharSequence content) {
        return newRequest(method, url, path, content != null ? ChannelBuffers.copiedBuffer(content, CharsetUtil.UTF_8) : null);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, BytesReference content) {
        return newRequest(method, url, path, content != null ? ChannelBuffers.copiedBuffer(content.toBytes()) : null);
    }

    protected HttpRequest newRequest(HttpMethod method, URL url, String path, ChannelBuffer buffer) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, path);
        request.headers().add(HttpHeaders.Names.HOST, url.getHost());
        request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.headers().add(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        if (buffer != null) {
            request.setContent(buffer);
            int length = request.getContent().readableBytes();
            request.headers().add(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, length);
        }
        return request;
    }

    protected void doExecute(final HttpContext<R, T> httpContext) {
        httpContext.getChannel().write(httpContext.getHttpRequest());
    }

    protected abstract HttpRequest createHttpRequest(URL base, R request) throws IOException;

    protected abstract T createResponse(HttpContext<R, T> httpContext) throws IOException;

}
