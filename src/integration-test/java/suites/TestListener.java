package suites;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 *
 */
public class TestListener extends RunListener {

    private static final Logger logger = LogManager.getLogger("test.listener");

    public void testRunStarted(Description description) throws java.lang.Exception {
        logger.info("number of tests to execute: {}", description.testCount());
    }

    public void testRunFinished(Result result) throws java.lang.Exception {
        logger.info("number of tests executed: {}", result.getRunCount());
    }

    public void testStarted(Description description) throws java.lang.Exception {
        logger.info("starting execution of {} {}",
                description.getClassName(), description.getMethodName());
    }

    public void testFinished(Description description) throws java.lang.Exception {
        logger.info("finished execution of {} {}",
                description.getClassName(), description.getMethodName());
    }

    public void testFailure(Failure failure) throws java.lang.Exception {
        logger.info("failed execution of tests: {}",
                failure.getMessage());
    }

    public void testIgnored(Description description) throws java.lang.Exception {
        logger.info("execution of test ignored: {}",
                description.getClassName(), description.getMethodName());
    }
}
