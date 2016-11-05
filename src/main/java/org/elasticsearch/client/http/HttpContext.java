package org.elasticsearch.client.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * HTTP context.
 *
 * @param <R> request type
 * @param <T> response type
 */
public class HttpContext<R extends ActionRequest<R>, T extends ActionResponse> {

    final R request;
    private final HttpAction<R, T> httpAction;
    ActionListener<T> listener;
    HttpRequest httpRequest;
    long millis;
    private Channel channel;
    private HttpResponse httpResponse;

    HttpContext(HttpAction<R, T> httpAction, ActionListener<T> listener, R request) {
        this.httpAction = httpAction;
        this.listener = listener;
        this.request = request;
    }

    public HttpAction<R, T> getHttpAction() {
        return httpAction;
    }

    public ActionListener<T> getListener() {
        return listener;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

}
