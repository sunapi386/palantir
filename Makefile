run:
	javac -cp src/com/palantir/GoodAgg src/com/palantir/GoodAgg/*.java
	java -Xms10G -Xmx10G -cp src com.palantir.GoodAgg.GoodAgg
