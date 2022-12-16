cd ..
javac -cp jar/*:. -Xlint Client.java
java -cp jar/*:. Client $1 $2 $3
