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

    private long duplicates;
    private long primes;

    private final double ALERT_THRESHOLD;
    private final boolean ALERT_ON_THRESHOLD_ENABLED;
    private final boolean ALERT_ON_DUPLICATE_ENABLED;
    private ScheduledExecutorService scheduler;

    private long sum = 0;

    public Analytics(Config config) {
        ALERT_THRESHOLD = config.getDouble("application.alert.duplicateRatioThreshold");
        ALERT_ON_THRESHOLD_ENABLED = config.getBoolean("application.alert.alertOnThreshold");
        ALERT_ON_DUPLICATE_ENABLED = config.getBoolean("application.alert.alertOnDuplicate");

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

    protected synchronized void calcStats() {
        double ratioOfDuplicates = (double)(duplicates) / (double)(primes);
        String percentageOfDuplicates = String.format("%.2f", ratioOfDuplicates * 100);
        if (ratioOfDuplicates > 1.0) {
            // We've received more duplicates than actual messages, something's wrong?
            // Either the feeds have more duplicates we've assumed or our hashing algorithm is not good enough
            log.error("Alert, we've received more duplicates than primary messages. primary: {}, duplicate: {}. percentage: {}%",
                    primes, duplicates, percentageOfDuplicates);
        }
        else if (ALERT_ON_THRESHOLD_ENABLED && ratioOfDuplicates < ALERT_THRESHOLD) {
            //We want to monitor that both feeds are up. In theory these two numbers should be nearly identical in the long term
            log.error("Alert, we haven't received enough duplicates, another feed down?! primary: {}, duplicate: {}. percentage: {}%",
                    primes, duplicates, percentageOfDuplicates);
        }

        double averageDelay = (double)sum / (double) duplicates;
        log.info("Percentage of getting both events is {} % with average delay of {} ms", percentageOfDuplicates, averageDelay);
        duplicates = 0;
        primes = 0;
        sum = 0;
    }

    public synchronized void reportDuplicate(long elapsedBetweenHits) {
        duplicates++;
        sum += elapsedBetweenHits;
        if (ALERT_ON_DUPLICATE_ENABLED) {
            log.error("Alert, received a duplicate with {} ms in between!", elapsedBetweenHits);
        }
    }

    public synchronized void reportPrime() {
        primes++;
    }

    public void close() {
        scheduler.shutdown();
    }
}
