package fi.hsl.transitdata.hfp;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting Hfp De-duplicator");
        Config config = ConfigParser.createConfig();
        Analytics analytics = null;
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();
            analytics = new Analytics(context.getConfig());

            Deduplicator router = new Deduplicator(context, analytics);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
            if (analytics != null)
                analytics.close();
        }
    }
}
