package org.elasticsearch.action.index;

import org.elasticsearch.action.GenericAction;
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
 */
public class HttpIndexAction extends HttpAction<IndexRequest, IndexResponse> {
    @Override
    public GenericAction<IndexRequest, IndexResponse> getActionInstance() {
        return IndexAction.INSTANCE;
    }

    @Override
    protected HttpRequest createHttpRequest(URL base, IndexRequest request) throws IOException {
        return newPutRequest(base, request.index() + "/" + request.type() + "/" + request.id(), request.source().toUtf8());
    }

    @Override
    protected IndexResponse createResponse(HttpContext<IndexRequest, IndexResponse> httpContext) throws IOException {
        HttpResponse httpResponse = httpContext.getHttpResponse();
        BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(ref).map();
        logger.info("{}", map);
        IndexResponse indexResponse = new IndexResponse();
        return indexResponse;
    }
}
