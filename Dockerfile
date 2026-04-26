from amazoncorretto:17
copy ./target/sko-0.0.1-SNAPSHOT.jar sko.jar
CMD ["java", "-jar", "sko.jar"]