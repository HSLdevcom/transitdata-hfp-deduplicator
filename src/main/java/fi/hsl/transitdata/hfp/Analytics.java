package fi.hsl.transitdata.hfp;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Analytics {

    private static final Logger log = LoggerFactory.getLogger(Analytics.class);

    private long hits;
    private long misses;

    private final double ALERT_THRESHOLD;
    private final boolean ALERT_ON_THRESHOLD_ENABLED;
    private final boolean ALERT_ON_DUPLICATE_ENABLED;
    private ScheduledExecutorService scheduler;

    private long sum = 0;

    public Analytics(Config config) {
        ALERT_THRESHOLD = config.getDouble("application.alert.duplicateRatioThreshold");
        ALERT_ON_THRESHOLD_ENABLED = config.getBoolean("application.alert.alertOnThreshold");
        ALERT_ON_DUPLICATE_ENABLED = config.getBoolean("application.alert.alertOnDuplicate"); //This is more for debugging

        Duration pollInterval = config.getDuration("application.alert.pollInterval");
        startPoller(pollInterval);
    }

    void startPoller(Duration interval) {
        long secs = interval.getSeconds();
        log.info("Analytics poll interval {} seconds", secs);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(this::calcStats,
                secs,
                secs,
                TimeUnit.SECONDS);
    }

    private synchronized void calcStats() {
        double percentageOfNotGettingBoth = Math.abs((double)(misses - hits) / (double)(misses));
        if (ALERT_ON_THRESHOLD_ENABLED && percentageOfNotGettingBoth >= ALERT_THRESHOLD) {
            //TODO think about this
            log.error("Alert, not getting both feeds!");
        }
        double averageDelay = (double)sum / (double)hits;
        log.info("Percentage of not getting both events is {} % with average delay of {} ms", percentageOfNotGettingBoth, averageDelay);
        hits = 0;
        misses = 0;
        sum = 0;
    }

    public synchronized void reportHit(long elapsedBetweenHits) {
        hits++;
        sum += elapsedBetweenHits;
        if (ALERT_ON_DUPLICATE_ENABLED) {
            log.error("Alert, received a duplicate with {} ms in between!", elapsedBetweenHits);
        }
    }

    public synchronized void reportMiss() {
        misses++;
    }

    public void close() {
        scheduler.shutdown();
    }
}
