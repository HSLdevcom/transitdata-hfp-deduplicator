package fi.hsl.transitdata.hfp;

import com.google.protobuf.ByteString;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.pulsar.ITBaseTestSuite;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarMessageData;
import fi.hsl.common.pulsar.TestPipeline;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ITDeduplicatorTest extends ITBaseTestSuite {
    @Test
    public void testDummyDuplicatesWithoutSchema() throws Exception {
        final String testId = "-test-duplicate-strings";
        PulsarApplication app = createPulsarApp("integration-test-dedup.conf", testId);
        Deduplicator dedup = newDeduplicator(app);

        ArrayList<PulsarMessageData> input = new ArrayList<>();
        ArrayList<PulsarMessageData> output = new ArrayList<>();
        for (int n = 0; n < 10; n++) {
            long ts = System.currentTimeMillis();

            String msg = "testme" + n;
            //varying number of inputs, but expecting each message only to arrive once.
            for (int times = 0; times <= n; times++) {
                //Vary the key to make sure we only receive the first one sent.
                String key = "jabadabaduu" + times;
                //Also timestamp should not matter, only the payload
                ts =+ 1;

                PulsarMessageData data = new PulsarMessageData(msg.getBytes(), ts, key);

                input.add(data);
                if (times == 0) {
                    logger.info("Adding msg {} with a key {} to output", msg, key);
                    output.add(data);
                }
            }
        }

        TestPipeline.MultiMessageTestLogic logic = new TestPipeline.MultiMessageTestLogic(input, output);
        testPulsarMessageHandler(dedup, app, logic, testId);
    }

    private Deduplicator newDeduplicator(PulsarApplication app) {
        //TODO add all required compontents to constructor
        //Requires a hook to ITBaseTestSuite which would be called on test-finished so we could shutdown the timer.
        return new Deduplicator(app.getContext(), null);
    }

    @Test
    public void testDuplicatesWithRawMqttSchema() throws Exception {
        final HashMap<String, String> properties = new HashMap<>();
        properties.put(TransitdataProperties.KEY_SCHEMA_VERSION, "1");
        properties.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.MqttRawMessage.toString());
        properties.put("foo", "bar");
        final long ts = System.currentTimeMillis(); //Let's use the same timestamp for all to ease testing.

        LinkedList<String> lines = readLinesFromResources("hfp-5000.txt");
        assertEquals(5000, lines.size());

        final List<Mqtt.RawMessage> sourcePayloads = lines.stream()
                .map(ITDeduplicatorTest::parseMqttRawMessage)
                .collect(Collectors.toList());
        final List<Mqtt.RawMessage> uniquePayloads = new LinkedList<>();

        HashMap<String, Integer> counter = new HashMap<>();
        for (Mqtt.RawMessage raw: sourcePayloads) {
            String key = raw.getTopic() + " " + new String(raw.getPayload().toByteArray());
            Integer prevCount = counter.get(key);
            if (prevCount == null) {
                counter.put(key, 1);
                uniquePayloads.add(raw);
            } else {
                logger.debug("Duplicate: " + key);
                counter.put(key, prevCount + 1);
            }
        }
        assertEquals(5000, sourcePayloads.size());
        assertEquals(4956, uniquePayloads.size());

        final String testId = "-test-raw-mqtt-duplicates";
        PulsarApplication app = createPulsarApp("integration-test-dedup.conf", testId);

        final Deduplicator dedup = newDeduplicator(app);

        final ArrayList<PulsarMessageData> input = sourcePayloads.stream().map(raw -> {
            byte[] data = raw.toByteArray();
            return new PulsarMessageData(data, ts, "hfp", properties);
        }).collect(Collectors.toCollection(ArrayList::new));

        final ArrayList<PulsarMessageData> output = uniquePayloads.stream().map(raw -> {
            byte[] data = raw.toByteArray();
            return new PulsarMessageData(data, ts, "hfp", properties);
        }).collect(Collectors.toCollection(ArrayList::new));

        logger.info("Sending {} hfp messages and expecting {} back", input.size(), output.size());
        TestPipeline.MultiMessageTestLogic logic = new TestPipeline.MultiMessageTestLogic(input, output);
        testPulsarMessageHandler(dedup, app, logic, testId);
    }

    LinkedList<String> readLinesFromResources(String filename) throws Exception {
        LinkedList<String> lines = new LinkedList<>();
        BufferedReader reader = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            URL url = classLoader.getResource(filename);
            reader = new BufferedReader(new InputStreamReader(url.openStream()));

            String line;
            while ((line = reader.readLine()) != null)
            {
                lines.add(line);
            }
            // close our reader
            reader.close();
        }
        finally {
            if (reader != null)
                reader.close();
        }
        return lines;
    }

    public static Mqtt.RawMessage parseMqttRawMessage(String line) {
        // Topic can unfortunately contain spaces so we need to parse the line using some heuristic methods.
        String[] splitted = line.split(" ", 2);
        final String serverTimestamp = splitted[0]; // not needed in this test
        final String topicAndPayload = splitted[1];

        int indexOfJsonStart = topicAndPayload.indexOf("{");

        final String topic = topicAndPayload.substring(0, indexOfJsonStart).trim();
        final String jsonPayload = topicAndPayload.substring(indexOfJsonStart);

        Mqtt.RawMessage.Builder builder = Mqtt.RawMessage.newBuilder();
        Mqtt.RawMessage raw = builder
                .setSchemaVersion(builder.getSchemaVersion())
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(jsonPayload.getBytes()))
                .build();
        return raw;
    }

}
