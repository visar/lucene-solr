package org.apache.solr.update;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.solr.schema.IndexSchema;

public class DefaultMergeSortFactory extends MergeSortFactory {

  /**
   * Factory for a {@link SortingMergePolicy} around an existing {@link MergePolicy}.
   * @param policy - the {@link MergePolicy} to be wrapped
   * @param schema - the {@link IndexSchema} to consult to obtain a {@link Sort}
   * @return a new {@link SortingMergePolicy}
   */
  public MergePolicy get(MergePolicy policy, IndexSchema schema) {
    Sort sort = schema.getMergeSortKeyFactory().getSort();
    if (null != sort) {
      return new SortingMergePolicy(policy, sort);
    }
    return null;
  }

}
