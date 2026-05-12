from amazoncorretto:17
copy ./target/simple-kafka-order-0.0.1-SNAPSHOT.jar simple-kafka-order.jar
CMD ["java", "-jar", "simple-kafka-order.jar"]