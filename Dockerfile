FROM openjdk:8-jdk-alpine
VOLUME /tmp
ARG JAR_FILE
RUN wget https://zenodo.org/record/8000664/files/app.jar
CMD java $JAVA_OPTIONS -jar /app.jar
