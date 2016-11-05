package org.elasticsearch.client.http;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.elasticsearch.node.NodeTestUtils;

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class HttpBulkClientTest extends NodeTestUtils {

    private static final ESLogger logger = ESLoggerFactory.getLogger(HttpBulkClientTest.class.getSimpleName());

    private static final Integer MAX_ACTIONS = 1000;

    private static final Long NUM_ACTIONS = 1234L;

    @Before
    public void startNodes() {
        try {
            super.startNodes();
        } catch (Exception t) {
            logger.error("startNodes failed", t);
        }
    }

    @Test
    public void testSingleDocHttpClient() throws Exception {
        try (HttpClient client = HttpClient.builder()
                .url(new URL("http://127.0.0.1:9200"))
                .build()) {
            CreateIndexRequestBuilder createIndexRequestBuilder =
                    new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE).setIndex("test");
            createIndexRequestBuilder.execute().actionGet();
            IndexRequestBuilder indexRequestBuilder =
                    new IndexRequestBuilder(client, IndexAction.INSTANCE)
                            .setIndex("test")
                            .setType("type")
                            .setId("1")
                            .setSource(jsonBuilder().startObject().field("name", "Hello World").endObject());
            indexRequestBuilder.execute().actionGet();
            RefreshRequestBuilder refreshRequestBuilder =
                    new RefreshRequestBuilder(client, RefreshAction.INSTANCE)
                            .setIndices("test");
            refreshRequestBuilder.execute().actionGet();
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertTrue(searchRequestBuilder.execute().actionGet().getHits().getTotalHits() > 0);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        }
    }

    @Test
    public void testNewIndexNodeClient() throws Exception {
        final HttpBulkClient client = HttpBulkClient.builder()
                .url(new URL("http://127.0.0.1:9200"))
                .build();
        client.newIndex("test");
        if (client.hasThrowable()) {
            logger.error("error", client.getThrowable());
        }
        assertFalse(client.hasThrowable());
        client.shutdown();
    }

    @Test
    public void testSingleDocBulkNodeClient() throws Exception {
        final HttpBulkClient client = HttpBulkClient.builder()
                .url(new URL("http://127.0.0.1:9200"))
                .build();
        try {
            client.newIndex("test");
            client.index("test", "test", "1", "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertTrue(searchRequestBuilder.execute().actionGet().getHits().getTotalHits() > 0) ;
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } catch (ExecutionException e) {
            logger.error(e.getMessage(), e);
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testRandomDocs() throws Exception {
        final HttpBulkClient client = HttpBulkClient.builder()
                .url(new URL("http://127.0.0.1:9200"))
                .maxActionsPerRequest(MAX_ACTIONS)
                .flushIngestInterval(TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertTrue(searchRequestBuilder.execute().actionGet().getHits().getTotalHits() > 0) ;
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testThreadedRandomDocs() throws Exception {
        int maxthreads = Runtime.getRuntime().availableProcessors();
        int maxactions = MAX_ACTIONS;
        final long maxloop = NUM_ACTIONS;
        logger.info("HttpBulkNodeClient max={} maxactions={} maxloop={}", maxthreads, maxactions, maxloop);
        final HttpBulkClient client = HttpBulkClient.builder()
                .url(new URL("http://127.0.0.1:9200"))
                .maxActionsPerRequest(maxactions)
                .flushIngestInterval(TimeValue.timeValueSeconds(60))
                .build();
        try {
            client.newIndex("test")
                    .startBulk("test", -1);
            ThreadPoolExecutor pool = EsExecutors.newFixed("http-bulk-nodeclient-test", maxthreads, 30,
                    EsExecutors.daemonThreadFactory("http-bulk-nodeclient-test"));
            final CountDownLatch latch = new CountDownLatch(maxthreads);
            for (int i = 0; i < maxthreads; i++) {
                pool.execute(new Runnable() {
                    public void run() {
                        for (int i = 0; i < maxloop; i++) {
                            client.index("test", "test", null, "{ \"name\" : \"" + randomString(32) + "\"}");
                        }
                        latch.countDown();
                    }
                });
            }
            logger.info("waiting for max 30 seconds...");
            latch.await(30, TimeUnit.SECONDS);
            logger.info("flush...");
            client.flushIngest();
            client.waitForResponses(TimeValue.timeValueSeconds(30));
            logger.info("got all responses, thread pool shutdown...");
            pool.shutdown();
            logger.info("pool is shut down");
            client.stopBulk("test", 1000);
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(0);
            assertEquals(maxthreads * maxloop,
                    searchRequestBuilder.execute().actionGet().getHits().getTotalHits());
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
        }
    }

}
