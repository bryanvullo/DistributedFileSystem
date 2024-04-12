/**
 * A Tuple data structure which can hold any 3 objects
 * @param <F> The type of the first Object
 * @param <S> The type of the second Object
 */
public class Pair<F, S> {
    
    private F first;
    private S second;
    
    /**
     * Constructor to create a new Triplet with 3 objects
     * @param first the first element
     * @param second the second element
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }
    
    /**
     * @return the first element
     */
    public F getFirst() {
        return first;
    }
    
    /**
     * @return the second element
     */
    public S getSecond() {
        return second;
    }
}
