package org.apache.lucene.search; // TODO: put elsewhere

public class IntegerRange {

  private final Integer min;
  private final Integer max;

  public IntegerRange(Integer min, Integer max) {
    this.min = min;
    this.max = max;
  }

  public boolean includes(int val) {
    return
        (null == min || min <= val) &&
        (null == max || val <= max);
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntegerRange))
      return false;
    IntegerRange that = (IntegerRange)o;

    if (this.min != null ? !this.min.equals(that.min) : that.min != null) return false;
    if (this.max != null ? !this.max.equals(that.max) : that.max != null) return false;

    return true;
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return (min == null ? 0 : min.hashCode()) ^ (max == null ? 0 : max.hashCode());
  }

  @Override
  public String toString() {
    return "["+(min==null?"*":min)+" TO "+(max==null?"*":max)+"]";
  }

}
