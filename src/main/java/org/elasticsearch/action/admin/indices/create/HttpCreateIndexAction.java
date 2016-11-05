package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.client.http.HttpAction;
import org.elasticsearch.client.http.HttpContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 *
 */
public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    @Override
    public CreateIndexAction getActionInstance() {
        return CreateIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, CreateIndexRequest request) {
        return newPostRequest(url, "/" + request.index(), null);
    }

    @Override
    protected CreateIndexResponse createResponse(HttpContext<CreateIndexRequest, CreateIndexResponse> httpContext)
            throws IOException {
        if (httpContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpContext.getHttpResponse();
        BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(ref).map();
        boolean acknowledged = map.containsKey("acknowledged") && (Boolean) map.get("acknowledged");
        return new CreateIndexResponse(acknowledged);
    }
}
