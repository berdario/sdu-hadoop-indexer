package test;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class WritableTuple2<T1 extends Writable, T2 extends Writable> implements Writable {
	T1 _1;
	T2 _2;
	
	//public WritableTuple2() {}
	
	public WritableTuple2(T1 _1, T2 _2) {
		this._1 = _1;
		this._2 = _2;
	}
	
	public void write(DataOutput out) throws IOException{
		_1.write(out);
		_2.write(out);
	}
	
	public void readFields(DataInput in) throws IOException{
		_1.readFields(in);
		_2.readFields(in);
	}
	
	@SuppressWarnings("unchecked")
	protected Class<? extends Writable>[] getTypes() {
		return new Class[]{_1.getClass(), _2.getClass()};
	}
}
