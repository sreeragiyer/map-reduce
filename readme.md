## Map Reduce library implementation

### Overview

- The library provides mapper, reducer abstract classes in the 'mapreduce/utils' directory in 'src'(source) folder for clients to extend the classes and execute user defined functions which have the actual business logic. 
- The input to the user defined mapper function is a key and value. The output of user defined mapper function is a map which is indexed on the key and contains the correspinding values for the key in the form of list.
- The abstract mapper class provided by the library first executes the user defined function to get the map of the key and corresponding values as list. It than writes all the data to a intermediate file 'intermediate_yyyyMMddHHmmss.txt' with each line having the key and corresponding value for reducer to process the data.
- The input to the user defined reducer function is the the key and corresponding list of values. The user defined function applies operations on the list of values for a corresponding key and returns the aggregated output back. 
- The abstract reducer class provided by the library first applies the reducer function to get the aggregated list of values for a key and than outputs the results to the specific output location provided by the user.
- The 'MapReduce' class present in the 'mapreduce/utils' directory in 'src'(source) folder plays the role of the master, clients execute the 'mapreduce' function from their main class to execute the application. 
- The 'mapReduce' function provided by the 'MapReduce' class takes as input the application specifications which contains the input, output, user defined mapper and reducer functions. It reads the data from the input file location and passes the data to the mapper function for executing the mapper logic, after the mapper execution is completed it runs the reducer by passing intermediate file location which has the data written by the mapper and after the reducer is completed it deletes the intermediate file created by the mapper.

### Test Cases

- All the test cases are present in the 'test_cases' directory in 'src'(source) folder. Each test case has three classes : Main class, Mapper class which has user defined mapper function and Reducer class which has user defined reducer function. The output of the test case is present in the 'test_case_output' folder.
- Each test case can be run by directly running the main class of the test case after compiling the project. Eg : java WordCount.java.
- The first application 'WordCount' takes as input a text file : 'hamlet.txt' present in the 'data' directory in 'src'(source) folder and returns the count for each word present in the text file. 
- The second application 'WordLength' takes as input a text file : 'loremipsum.txt' present in the 'data' directory in 'src'(source) folder and returns a text file which contains a fixed length and all the words corresponding to that fixed length.
- The third application 'WordinLine' takes as input a text file : 'hamlet.txt' present in the 'data' directory in 'src'(source) folder and returns a text file which contains each word along with its corresponding line number.

### How to run

The shell script to run all the test cases is present in the root directory (???run-test-cases.sh???). This will compile all the library code with test cases and then run them. The shell script will need to have permissions to run on the user???s machine.
```
./run-test-cases.sh
```

To run the spark implementation - the spark implementation is present in the src/spark folder. 
```
python3 filename.py
```
