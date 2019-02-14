package fi.hsl.transitdata.hfp;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.ITBaseTestSuite;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ITDeduplicatorTest extends ITBaseTestSuite {
    @Test
    public void testDummyDuplicatesWithoutSchema() throws Exception {
        final String testId = "-test-duplicates";
        PulsarApplication app = createPulsarApp("integration-test-dedup.conf", testId);

        Deduplicator dedup = new Deduplicator(app.getContext(), null);

        TestLogic logic = new TestLogic() {

            @Override
            public void testImpl(TestContext context) throws Exception {
                long now = System.currentTimeMillis();
                sendPulsarMessage(context.source, "key", now, "testme".getBytes(), null, null);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                assertNotNull(received);

                //validatePulsarProperties(received, "key", now, null);
                String readData = new String(received.getData());
                assertEquals("testme", readData);
                logger.info("Message read back, all good");

                validateAcks(1, context);
            }
        };
        testPulsarMessageHandler(dedup, app, logic, testId);
    }
}
