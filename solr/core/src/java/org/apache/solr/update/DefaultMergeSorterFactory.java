package org.apache.solr.update;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.sorter.Sorter;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.solr.schema.IndexSchema;

public class DefaultMergeSorterFactory extends MergeSorterFactory {
  
  /**
   * Factory for a {@link SortingMergePolicy} around an existing {@link MergePolicy}.
   * @param policy - the {@link MergePolicy} to be wrapped
   * @param schema - the {@link IndexSchema} to consult to obtain a {@link Sorter}
   * @return a new {@link SortingMergePolicy}
   */
  public MergePolicy get(MergePolicy policy, IndexSchema schema) {
    Sorter sorter = schema.getMergeSorterKeyFactory().getSorter();
    if (null != sorter) {
      return new SortingMergePolicy(policy, sorter);
    }
    return null;
  }
  
}
