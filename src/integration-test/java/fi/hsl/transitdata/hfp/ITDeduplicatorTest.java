package fi.hsl.transitdata.hfp;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.ITBaseTestSuite;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarMessageData;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ListIterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ITDeduplicatorTest extends ITBaseTestSuite {
    @Test
    public void testDummyDuplicatesWithoutSchema() throws Exception {
        final String testId = "-test-duplicate-strings";
        PulsarApplication app = createPulsarApp("integration-test-dedup.conf", testId);

        //TODO add all required compontents to constructor
        Deduplicator dedup = new Deduplicator(app.getContext(), null);

        ArrayList<PulsarMessageData> input = new ArrayList<>();
        ArrayList<PulsarMessageData> output = new ArrayList<>();
        for (int n = 0; n < 10; n++) {
            long ts = System.currentTimeMillis();
            String msg = "testme" + n;
            //varying number of inputs, but expecting each message only to arrive once.
            for (int times = 0; times <= n; times++) {
                //Vary the key to make sure we only receive the first one sent.
                String key = "jabadabaduu" + times;
                PulsarMessageData data = new PulsarMessageData(msg.getBytes(), ts, key);

                input.add(data);
                if (times == 0) {
                    logger.info("Adding msg {} with a key {} to output", msg, key);
                    output.add(data);
                }
            }
        }

        BufferedTestLogic logic = new BufferedTestLogic(input, output);
        testPulsarMessageHandler(dedup, app, logic, testId);
    }

    public static class BufferedTestLogic extends TestLogic {

        protected final ArrayList<PulsarMessageData> input;
        protected final ArrayList<PulsarMessageData> expectedOutput;

        public BufferedTestLogic(ArrayList<PulsarMessageData> in,
                                 ArrayList<PulsarMessageData> out) {
            input = in;
            expectedOutput = out;
            logger.info("Sending {} messages and expecting {} back", input.size(), expectedOutput.size());
        }

        @Override
        public void testImpl(TestContext context) throws Exception {
            //For simplicity let's just send all messages first and then read them back.
            logger.info("Sending {} messages", input.size());
            long now = System.currentTimeMillis();

            for(PulsarMessageData inputData : input) {
                TypedMessageBuilder<byte[]> msg = PulsarMessageData.toPulsarMessage(context.source, inputData);
                msg.send();
            }
            logger.info("Messages sent in {} ms, reading them back", (System.currentTimeMillis() - now));

            final long expectedCount = expectedOutput.size();
            ArrayList<Message<byte[]>> buffer = new ArrayList<>();
            now = System.currentTimeMillis();
            while (buffer.size() < expectedCount) {
                Message<byte[]> read = ITBaseTestSuite.readOutputMessage(context);
                assertNotNull("Was expecting more messages but got null!", read);
                buffer.add(read);
            }
            logger.info("{} messages read back in {} ms", buffer.size(), (System.currentTimeMillis() - now));

            assertEquals(expectedCount, buffer.size());
            //All input messages should have been acked.
            ITBaseTestSuite.validateAcks(input.size(), context);

            validateOutput(buffer);
        }

        protected void validateOutput(ArrayList<Message<byte[]>> receivedQueue) {
            assertEquals(expectedOutput.size(), receivedQueue.size());
            ListIterator<Message<byte[]>> itrRecv = receivedQueue.listIterator();
            ListIterator<PulsarMessageData> itrExp = expectedOutput.listIterator();

            while (itrRecv.hasNext()) {
                Message<byte[]> receivedMsg = itrRecv.next();
                PulsarMessageData received = PulsarMessageData.fromPulsarMessage(receivedMsg);
                PulsarMessageData expected = itrExp.next();

                validateMessage(expected, received);
            }
        }

        /**
         * Override this for your own check if needed
         */
        protected void validateMessage(PulsarMessageData expected, PulsarMessageData received) {
            assertNotNull(expected);
            assertNotNull(received);
            assertEquals(expected, received);
        }
    }
}
