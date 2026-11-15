# Etap 1: budowanie bundli OSGi i dystrybucji Karafa
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
RUN mvn -B -q -DskipTests package

# Etap 2: sam Karaf z zainstalowanymi bundlami
FROM eclipse-temurin:17-jre
COPY --from=build /src/pricing-distribution/target/assembly /opt/karaf
EXPOSE 8090 8091 8101
ENTRYPOINT ["/opt/karaf/bin/karaf", "run"]
