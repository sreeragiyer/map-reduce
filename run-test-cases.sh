
#javac src/**/*.java
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  find -name "*.java" > sources.txt
  javac @sources.txt
else
  javac src/**/*.java
fi
java -cp ./src test_cases/wordlength/WordLength
java -cp ./src test_cases/wordinline/WordinLine
java -cp ./src test_cases/wordcount/WordCount
