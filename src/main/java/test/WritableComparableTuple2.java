package test;

import org.apache.hadoop.io.WritableComparable;

public class WritableComparableTuple2<T1 extends WritableComparable<? super T1>, T2 extends WritableComparable<? super T2>> 
	extends WritableTuple2<T1, T2> 
	implements WritableComparable<WritableComparableTuple2<T1, T2>> {
	
	public WritableComparableTuple2() {}
	
	public WritableComparableTuple2(T1 _1, T2 _2) {
		super(_1, _2);
	}

	public int compareTo(WritableComparableTuple2<T1,T2> o){
		int result = _1.compareTo(o._1);
		if (result == 0){
			return _2.compareTo(o._2);
		}
		return result;
	}

}
