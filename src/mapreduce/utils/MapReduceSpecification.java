package mapreduce.utils;

public class MapReduceSpecification {
    /**
     * This is the constant 'N' which will indicate the number of mapper
     * and reducer processes to be launched.
     */
    public int numProcesses;
    /**
     * Takes the input file location where the input data for map reduce
     * is present.
     */
    public String inputFileLocation;
    /**
     * Takes the output file location where the output is writtern after the
     * map reduce execution is completed.
     */
    public String outputFileLocation;
    /**
     * mapreduce.utils.Mapper function which will do the mapping execution.
     */
    public Mapper mapper;
    /**
     * mapreduce.utils.Reducer function which will do the reducer execution.
     */
    public Reducer reducer;
    /**
     * key to user defined mapper object for the rmi registry
     */
    public String mapperKey;
    /**
     * key to user defined reducer object for the rmi registry
     */
    public String reducerKey;
}
