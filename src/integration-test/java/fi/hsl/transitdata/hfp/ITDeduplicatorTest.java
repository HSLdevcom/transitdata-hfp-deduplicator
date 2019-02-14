package fi.hsl.transitdata.hfp;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.ITBaseTestSuite;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
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
                sendStringMessage(context, "key", "testme", now);
                sendStringMessage(context, "key", "testme", now);
                sendStringMessage(context, "key", "testme2", now);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                assertNotNull(received);
                Message<byte[]> received2 = readOutputMessage(context);
                assertNotNull(received2);

                //validatePulsarProperties(received, "key", now, null);
                String readData = new String(received.getData());
                assertEquals("testme", readData);
                String readData2 = new String(received2.getData());
                assertEquals("testme2", readData2);
                logger.info("Message read back, all good");

                validateAcks(3, context);
            }
        };
        testPulsarMessageHandler(dedup, app, logic, testId);
    }

    private void sendStringMessage(TestContext context, String key, String payload, long now) throws PulsarClientException {
        sendPulsarMessage(context.source, key, now, payload.getBytes(), null, null);

    }
}
