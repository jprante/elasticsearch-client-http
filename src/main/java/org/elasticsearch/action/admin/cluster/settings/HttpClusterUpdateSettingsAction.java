package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.client.http.HttpAction;
import org.elasticsearch.client.http.HttpContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class HttpClusterUpdateSettingsAction extends HttpAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    @Override
    public ClusterUpdateSettingsAction getActionInstance() {
        return ClusterUpdateSettingsAction.INSTANCE;
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, ClusterUpdateSettingsRequest request) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject().startObject("persistent");
        request.persistentSettings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.startObject("transient");
        request.transientSettings().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject().endObject();
        return newRequest(HttpMethod.PUT, url, "/_cluster/settings", builder.string());
    }

    @Override
    protected ClusterUpdateSettingsResponse
            createResponse(HttpContext<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> httpContext)
            throws IOException {
        if (httpContext == null) {
            throw new IllegalStateException("no http context");
        }
        return new ClusterUpdateSettingsResponse();
    }
}
