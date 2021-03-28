
#javac src/**/*.java
find -name "*.java" > sources.txt
javac @sources.txt
java -cp ./src test_cases/wordlength/WordLength
java -cp ./src test_cases/capitalize/Capitalize
java -cp ./src test_cases/wordcount/WordCount
