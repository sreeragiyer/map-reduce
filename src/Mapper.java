import java.util.HashMap;

public abstract class Mapper<K1, V1, K2, V2> {

    protected abstract HashMap<K2, V2> map(K1 k, V1 v);
    
}
