package org.elasticsearch.action.search;

import org.elasticsearch.client.http.HttpAction;
import org.elasticsearch.client.http.HttpContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.InternalProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;

import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    private static final String SCROLL_ID = "_scroll_id";
    private static final String TOOK = "took";
    private static final String SHARDS = "_shards";
    private static final String TOTAL = "total";
    private static final String SUCCESSFUL = "successful";
    private static final String HITS = "hits";
    private static final String MAXSCORE = "max_score";

    @Override
    public SearchAction getActionInstance() {
        return SearchAction.INSTANCE;
    }

    @Override
    protected HttpRequest createHttpRequest(URL url, SearchRequest request) throws IOException {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newRequest(HttpMethod.POST, url, index + "/_search", request.extraSource());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SearchResponse createResponse(HttpContext<SearchRequest, SearchResponse> httpContext) throws IOException {
        if (httpContext == null) {
            throw new IllegalStateException("no http context");
        }
        HttpResponse httpResponse = httpContext.getHttpResponse();
        logger.info("{}", httpResponse.getContent().toString(CharsetUtil.UTF_8));
        BytesReference ref = new ChannelBufferBytesReference(httpResponse.getContent());
        Map<String, Object> map = JsonXContent.jsonXContent.createParser(ref).map();

        logger.info("{}", map);

        InternalSearchResponse internalSearchResponse = parseInternalSearchResponse(map);
        String scrollId = (String) map.get(SCROLL_ID);
        int totalShards = 0;
        int successfulShards = 0;
        if (map.containsKey(SHARDS)) {
            Map<String, ?> shards = (Map<String, ?>) map.get(SHARDS);
            totalShards = shards.containsKey(TOTAL) ? (Integer) shards.get(TOTAL) : -1;
            successfulShards = shards.containsKey(SUCCESSFUL) ? (Integer) shards.get(SUCCESSFUL) : -1;
        }
        int tookInMillis = map.containsKey(TOOK) ? (Integer) map.get(TOOK) : -1;
        ShardSearchFailure[] shardFailures = null;
        return new SearchResponse(internalSearchResponse, scrollId, totalShards, successfulShards, tookInMillis, shardFailures);
    }

    private InternalSearchResponse parseInternalSearchResponse(Map<String, ?> map) {
        InternalSearchHits internalSearchHits = parseInternalSearchHits(map);
        InternalAggregations internalAggregations = null;
        Suggest suggest = null;
        InternalProfileShardResults internalProfileShardResults = null;
        Boolean timeout = false;
        Boolean terminatedEarly = false;
        return new InternalSearchResponse(internalSearchHits,
                internalAggregations,
                suggest,
                internalProfileShardResults,
                timeout,
                terminatedEarly);
    }

    @SuppressWarnings("unchecked")
    private InternalSearchHits parseInternalSearchHits(Map<String, ?> map) {
        // InternalSearchHits(InternalSearchHit[] hits, long totalHits, float maxScore)
        InternalSearchHit[] internalSearchHits = parseInternalSearchHit();
        Map<String, ?> internalMap = (Map<String, ?>) map.get(HITS);
        long totalHits = internalMap != null && internalMap.containsKey(TOTAL) ? (Integer) internalMap.get(TOTAL) : -1L;
        double maxScore = internalMap != null && internalMap.containsKey(MAXSCORE) ? (Double) internalMap.get(MAXSCORE) : 0.0f;
        return new InternalSearchHits(internalSearchHits, totalHits, (float) maxScore);
    }

    @SuppressWarnings("unchecked")
    private InternalSearchHit[] parseInternalSearchHit() {
        // InternalSearchHit(int docId, String id, Text type, Map<String, SearchHitField> fields)
        List<InternalSearchHit> list = new LinkedList<>();
        return list.toArray(new InternalSearchHit[list.size()]);
    }

}

