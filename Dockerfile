FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY src/ src/
RUN mkdir -p out && javac -d out src/main/java/org/example/NodoWeb.java
RUN cp src/main/resources/index.html out/
RUN cp src/main/resources/sw.js out/
RUN cp src/main/resources/manifest.json out/

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/out/ .
EXPOSE 8080
CMD ["java", "-cp", ".", "org.example.NodoWeb"]
