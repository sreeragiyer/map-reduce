public abstract class Reducer<K2, V2> {

    protected abstract V2[] reduce(K2 k, V2[] v);
    
}
