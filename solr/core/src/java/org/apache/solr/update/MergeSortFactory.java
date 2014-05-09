package org.apache.solr.update;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.schema.IndexSchema;

public abstract class MergeSortFactory {

/**
 * Factory for a {@link MergePolicy} around an existing {@link MergePolicy}.
 * @param policy - the {@link MergePolicy} to be wrapped
 * @param schema - the {@link IndexSchema} to consult (if required)
 * @return a new {@link MergePolicy}
 */
  public abstract MergePolicy get(MergePolicy policy, IndexSchema schema);

}
