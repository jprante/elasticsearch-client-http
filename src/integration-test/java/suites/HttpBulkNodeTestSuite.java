package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.elasticsearch.client.http.HttpBulkClientTest;

/**
 *
 */
@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        HttpBulkClientTest.class
})
public class HttpBulkNodeTestSuite {

}
