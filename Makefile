run:
	javac -cp src/com/palantir/BadAgg src/com/palantir/BadAgg/*.java
	java -Xms10G -Xmx10G -cp src com.palantir.BadAgg.BadAgg
