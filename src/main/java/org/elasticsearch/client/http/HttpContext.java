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
 * @param <Request> request type
 * @param <Response> response type
 */
public class HttpContext<Request extends ActionRequest<Request>, Response extends ActionResponse> {

    final Request request;
    private final HttpAction<Request, Response> httpAction;
    ActionListener<Response> listener;
    HttpRequest httpRequest;
    long millis;
    private Channel channel;
    private HttpResponse httpResponse;

    HttpContext(HttpAction<Request, Response> httpAction, ActionListener<Response> listener, Request request) {
        this.httpAction = httpAction;
        this.listener = listener;
        this.request = request;
    }

    public HttpAction<Request, Response> getHttpAction() {
        return httpAction;
    }

    public ActionListener<Response> getListener() {
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
