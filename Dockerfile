FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

COPY target/transitdata-hfp-deduplicator-jar-with-dependencies.jar /usr/app/transitdata-hfp-deduplicator.jar

ENTRYPOINT ["java", "-XX:InitialRAMPercentage=10.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitdata-hfp-deduplicator.jar"]
