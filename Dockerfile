# syntax=docker/dockerfile:1
# check=error=true

FROM eclipse-temurin:11-alpine

COPY target/transitdata-hfp-deduplicator.jar /usr/app/transitdata-hfp-deduplicator.jar

ENTRYPOINT ["java", "-XX:InitialRAMPercentage=10.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitdata-hfp-deduplicator.jar"]
