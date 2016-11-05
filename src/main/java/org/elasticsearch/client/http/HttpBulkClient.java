package org.elasticsearch.client.http;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class HttpBulkClient {

    private static final ESLogger logger = ESLoggerFactory.getLogger(HttpBulkClient.class.getName());

    private ElasticsearchClient client;

    private HttpBulkProcessor bulkProcessor;

    private Throwable throwable;

    private boolean closed;

    private Settings.Builder settingsBuilder;

    private Settings settings;

    private Map<String, String> mappings = new HashMap<>();

    private HttpBulkClient(ElasticsearchClient client, HttpBulkProcessor httpBulkProcessor) {
        this.bulkProcessor = httpBulkProcessor;
        this.client = client;
        this.closed = false;
    }

    public static Builder builder() {
        return new Builder();
    }

    public ElasticsearchClient client() {
        return client;
    }

    public void put(String key, String value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void put(String key, Boolean value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void put(String key, Integer value) {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        settingsBuilder.put(key, value);
    }

    public void put(InputStream in) throws IOException {
        settingsBuilder = Settings.settingsBuilder().loadFromStream(".json", in);
    }

    public Settings.Builder getSettingsBuilder() {
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        return settingsBuilder;
    }

    public Settings getSettings() {
        if (settings != null) {
            return settings;
        }
        if (settingsBuilder == null) {
            settingsBuilder = Settings.settingsBuilder();
        }
        return settingsBuilder.build();
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public void mapping(String type, String mapping) throws IOException {
        mappings.put(type, mapping);
    }

    public void mapping(String type, InputStream in) throws IOException {
        if (type == null) {
            return;
        }
        StringWriter sw = new StringWriter();
        Streams.copy(new InputStreamReader(in), sw);
        mappings.put(type, sw.toString());
    }

    public Map<String, String> mappings() {
        return mappings.isEmpty() ? null : mappings;
    }

    public void updateIndexSetting(String index, String key, Object value) throws IOException {
        if (client() == null) {
            return;
        }
        if (index == null) {
            throw new IOException("no index name given");
        }
        if (key == null) {
            throw new IOException("no key given");
        }
        if (value == null) {
            throw new IOException("no value given");
        }
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(settingsBuilder);
        client().execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    public void waitForRecovery() throws IOException {
        if (client() == null) {
            return;
        }
        client().execute(RecoveryAction.INSTANCE, new RecoveryRequest()).actionGet();
    }

    public int waitForRecovery(String index) throws IOException {
        if (client() == null) {
            return -1;
        }
        if (index == null) {
            throw new IOException("unable to waitfor recovery, index not set");
        }
        RecoveryResponse response = client().execute(RecoveryAction.INSTANCE, new RecoveryRequest(index)).actionGet();
        int shards = response.getTotalShards();
        client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest(index).waitForActiveShards(shards)).actionGet();
        return shards;
    }

    public void waitForCluster(String statusString, TimeValue timeout) throws IOException {
        if (client() == null) {
            return;
        }
        try {
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusString);
            ClusterHealthResponse healthResponse =
                    client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest()
                            .waitForStatus(status).timeout(timeout)).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", from here on, everything will fail!");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue");
        }
    }

    public String getClusterName() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterStateRequestBuilder clusterStateRequestBuilder =
                    new ClusterStateRequestBuilder(client(), ClusterStateAction.INSTANCE).all();
            ClusterStateResponse clusterStateResponse = clusterStateRequestBuilder.execute().actionGet();
            String name = clusterStateResponse.getClusterName().value();
            int nodeCount = clusterStateResponse.getState().getNodes().size();
            return name + " (" + nodeCount + " nodes connected)";
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public String healthColor() {
        if (client() == null) {
            return null;
        }
        try {
            ClusterHealthResponse healthResponse =
                    client().execute(ClusterHealthAction.INSTANCE, new ClusterHealthRequest()
                            .timeout(TimeValue.timeValueSeconds(30))).actionGet();
            ClusterHealthStatus status = healthResponse.getStatus();
            return status.name();
        } catch (ElasticsearchTimeoutException e) {
            return "TIMEOUT";
        } catch (NoNodeAvailableException e) {
            return "DISCONNECTED";
        } catch (Throwable t) {
            return "[" + t.getMessage() + "]";
        }
    }

    public int updateReplicaLevel(String index, int level) throws IOException {
        waitForCluster("YELLOW", TimeValue.timeValueSeconds(30));
        updateIndexSetting(index, "number_of_replicas", level);
        return waitForRecovery(index);
    }

    public void flushIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(FlushAction.INSTANCE, new FlushRequest(index)).actionGet();
        }
    }

    public void refreshIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }

    public void putMapping(String index) {
        if (client() == null) {
            return;
        }
        if (!mappings().isEmpty()) {
            for (Map.Entry<String, String> me : mappings().entrySet()) {
                client().execute(PutMappingAction.INSTANCE,
                        new PutMappingRequest(index).type(me.getKey()).source(me.getValue())).actionGet();
            }
        }
    }

    public String resolveAlias(String alias) {
        if (client() == null) {
            return alias;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    public void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep) {
        if (client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(client(), GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        Set<String> indices = new TreeSet<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches()) {
                if (index.equals(m.group(1)) && !s.equals(concreteIndex)) {
                    indices.add(s);
                }
            }
        }
        if (indices.isEmpty()) {
            logger.info("no indices found, retention policy skipped");
            return;
        }
        if (mintokeep > 0 && indices.size() <= mintokeep) {
            logger.info("{} indices found, not enough for retention policy ({}),  skipped",
                    indices.size(), mintokeep);
            return;
        } else {
            logger.info("candidates for deletion = {}", indices);
        }
        List<String> indicesToDelete = new ArrayList<>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = indices.size() - indicesToDelete.size();
                    if ((timestampdiff == 0 || (timestampdiff > 0 && i1 - i2 > timestampdiff)) && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        logger.info("indices to delete = {}", indicesToDelete);
        if (indicesToDelete.isEmpty()) {
            logger.info("not enough indices found to delete, retention policy complete");
            return;
        }
        String[] s = indicesToDelete.toArray(new String[indicesToDelete.size()]);
        DeleteIndexRequestBuilder requestBuilder = new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

    public Long mostRecentDocument(String index) {
        if (client() == null) {
            return null;
        }
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client(), SearchAction.INSTANCE);
        SortBuilder sort = SortBuilders.fieldSort("_timestamp").order(SortOrder.DESC);
        SearchResponse searchResponse = searchRequestBuilder.setIndices(index).addField("_timestamp")
                .setSize(1).addSort(sort).execute().actionGet();
        if (searchResponse.getHits().getHits().length == 1) {
            SearchHit hit = searchResponse.getHits().getHits()[0];
            if (hit.getFields().get("_timestamp") != null) {
                return hit.getFields().get("_timestamp").getValue();
            } else {
                return 0L;
            }
        }
        return null;
    }

    public HttpBulkClient index(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient delete(String index, String type, String id) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient update(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(new UpdateRequest().index(index).type(type).id(id).upsert(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of update request failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient flushIngest() {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        logger.debug("flushing bulk processor");
        bulkProcessor.flush();
        return this;
    }

    public HttpBulkClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException, ExecutionException {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        while (!bulkProcessor.awaitClose(maxWaitTime.getMillis(), TimeUnit.MILLISECONDS)) {
            logger.warn("still waiting for responses");
        }
        return this;
    }

    public HttpBulkClient startBulk(String index, long startRefreshIntervalMillis) throws IOException {
        updateIndexSetting(index, "refresh_interval", startRefreshIntervalMillis + "ms");
        return this;
    }

    public HttpBulkClient stopBulk(String index, long stopRefreshItervalMillis) throws IOException {
        updateIndexSetting(index, "refresh_interval", stopRefreshItervalMillis + "ms");
        return this;
    }

    public synchronized void shutdown() {
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public HttpBulkClient newIndex(String index) {
        return newIndex(index, null, null);
    }

    public HttpBulkClient newIndex(String index, String type, InputStream settings, InputStream mappings)
            throws IOException {
        put(settings);
        mapping(type, mappings);
        return newIndex(index, getSettings(), mappings());
    }

    public HttpBulkClient newIndex(String index, Settings settings, Map<String, String> mappings) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client(), CreateIndexAction.INSTANCE).setIndex(index);
        if (settings != null) {
            logger.info("settings for creating index {} = {}", index, settings.getAsStructuredMap());
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mappings != null) {
            for (String type : mappings.keySet()) {
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, mappings.get(type));
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", index);
        return this;
    }

    public HttpBulkClient newMapping(String index, String type, Map<String, Object> mapping) {
        PutMappingRequestBuilder putMappingRequestBuilder =
                new PutMappingRequestBuilder(client(), PutMappingAction.INSTANCE)
                        .setIndices(index)
                        .setType(type)
                        .setSource(mapping);
        putMappingRequestBuilder.execute().actionGet();
        logger.info("mapping created for index {} and type {}", index, type);
        return this;
    }

    public HttpBulkClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client(), DeleteIndexAction.INSTANCE, index);
        deleteIndexRequestBuilder.execute().actionGet();
        return this;
    }

    public boolean hasThrowable() {
        return throwable != null;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public static class Builder {

        private Settings settings = Settings.EMPTY;

        private URL url;

        private String host;

        private Integer port;

        private int maxActionsPerRequest = 1000;

        private int maxConcurrentRequests = Runtime.getRuntime().availableProcessors() * 4;

        private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

        private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

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

        public Builder maxActionsPerRequest(int maxActionsPerRequest) {
            this.maxActionsPerRequest = maxActionsPerRequest;
            return this;
        }

        public Builder maxConcurrentRequests(int maxConcurrentRequests) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            return this;
        }

        public Builder maxVolumePerRequest(ByteSizeValue maxVolume) {
            this.maxVolume = maxVolume;
            return this;
        }

        public Builder flushIngestInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public HttpBulkClient build() {
            Settings effectiveSettings = Settings.builder().put(settings)
                    .put("node.client", true)
                    .put("node.master", false)
                    .put("node.data", false).build();
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
            HttpClient client = HttpClient.builder().settings(effectiveSettings).url(url).build();
            HttpBulkProcessor.Listener listener = new HttpBulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.debug("before bulk [{}] [actions={}] [bytes={}]",
                            executionId,
                            request.numberOfActions(),
                            request.estimatedSizeInBytes());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    int n = 0;
                    for (BulkItemResponse itemResponse : response.getItems()) {
                        if (itemResponse.isFailed()) {
                            n++;
                        }
                    }
                    logger.debug("after bulk [{}] [{}ms]",
                            executionId,
                            response.getTook().millis());
                    if (n > 0) {
                        logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                                executionId, n, response.buildFailureMessage());
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("after bulk [" + executionId + "] error", failure);
                }
            };
            HttpBulkProcessor.Builder builder = HttpBulkProcessor.builder(effectiveSettings, client, listener)
                    .setBulkActions(maxActionsPerRequest)
                    .setConcurrentRequests(maxConcurrentRequests)
                    .setFlushInterval(flushInterval);
            if (maxVolume != null) {
                builder.setBulkSize(maxVolume);
            }
            return new HttpBulkClient(client, builder.build());
        }
    }

}
