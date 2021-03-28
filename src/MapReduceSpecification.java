import main.mapreduce.Mapper;

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
     * Mapper function which will do the mapping execution.
     */
    Mapper mapper;
    /**
     * Reducer function which will do the reducer execution.
     */
    Reducer reducer;
}
