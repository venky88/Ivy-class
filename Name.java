package org.learning.spark.ivy.sample;

public class Name {
	String name;
	Double count;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getCount() {
		return count;
	}

	public void setCount(Double count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "Name [name=" + name + ", count=" + count + "]";
	}

}
