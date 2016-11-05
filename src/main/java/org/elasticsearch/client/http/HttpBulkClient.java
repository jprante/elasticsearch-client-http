package org.elasticsearch.client.http;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HttpBulkClient {

    private static final ESLogger logger = ESLoggerFactory.getLogger(HttpBulkClient.class.getName());

    private ElasticsearchClient client;

    private HttpBulkProcessor bulkProcessor;

    private Exception exception;

    private boolean closed;

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
        Settings.Builder builder = Settings.settingsBuilder();
        builder.put(key, value.toString());
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index)
                .settings(builder);
        client().execute(UpdateSettingsAction.INSTANCE, updateSettingsRequest).actionGet();
    }

    public void refreshIndex(String index) {
        if (client() == null) {
            return;
        }
        if (index != null) {
            client().execute(RefreshAction.INSTANCE, new RefreshRequest(index)).actionGet();
        }
    }

    public HttpBulkClient index(String index, String type, String id, String source) {
        return bulkIndex(new IndexRequest(index).type(type).id(id).create(false).source(source));
    }

    public HttpBulkClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            exception = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient delete(String index, String type, String id) {
        return bulkDelete(new DeleteRequest(index).type(type).id(id));
    }

    public HttpBulkClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            exception = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        }
        return this;
    }

    public HttpBulkClient update(String index, String type, String id, String source) {
        return bulkUpdate(new UpdateRequest().index(index).type(type).id(id).upsert(source));
    }

    public HttpBulkClient bulkUpdate(UpdateRequest updateRequest) {
        if (closed) {
            throw new ElasticsearchException("client is closed");
        }
        try {
            bulkProcessor.add(updateRequest);
        } catch (Exception e) {
            exception = e;
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
            for (Map.Entry<String,String> entry : mappings.entrySet()) {
                String type = entry.getKey();
                logger.info("found mapping for {}", type);
                createIndexRequestBuilder.addMapping(type, entry.getValue());
            }
        }
        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", index);
        return this;
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception getException() {
        return exception;
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
                    this.host = settings.get("host", "localhost");
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
