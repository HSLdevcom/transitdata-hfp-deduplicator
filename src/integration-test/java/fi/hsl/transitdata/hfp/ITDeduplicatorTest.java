package fi.hsl.transitdata.hfp;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.ITBaseTestSuite;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;

import java.util.ArrayList;

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

    public static class BufferedTestLogic extends TestLogic {

        protected final ArrayList<TypedMessageBuilder<byte[]>> input;
        protected final ArrayList<TypedMessageBuilder<byte[]>> expectedOutput;

        public BufferedTestLogic(ArrayList<TypedMessageBuilder<byte[]>> in,
                                      ArrayList<TypedMessageBuilder<byte[]>> out) {
            input = in;
            expectedOutput = out;
        }

        @Override
        public void testImpl(TestContext context) throws Exception {
            //For simplicity let's just send all messages first and then read them back.
            logger.info("Sending {} messages", input.size());
            long now = System.currentTimeMillis();
            for(TypedMessageBuilder<byte[]> msg : input) {
                msg.send();
            }
            logger.info("Messages sent in {} ms, reading them back", (System.currentTimeMillis() - now));

            final long expectedCount = expectedOutput.size();
            ArrayList<Message<byte[]>> buffer = new ArrayList<>();
            now = System.currentTimeMillis();
            while (buffer.size() < expectedCount) {
                Message<byte[]> read = readOutputMessage(context);
                assertNotNull("Was expecting more messages but got null!", read);
                buffer.add(read);
            }
            logger.info("{} messages read back in {} ms", buffer.size(), (System.currentTimeMillis() - now));

            assertEquals(expectedCount, buffer.size());
            //All input messages should have been acked.
            validateAcks(input.size(), context);

            validateOutput(buffer);
        }

        protected void validateOutput(ArrayList<Message<byte[]>> received) {
            //Override this for your own custom check

        }


    }
}
