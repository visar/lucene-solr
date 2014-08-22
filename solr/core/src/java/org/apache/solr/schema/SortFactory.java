package org.apache.solr.schema;
/*
 * Modelled on SimilarityFactory class.
 */

import org.apache.lucene.index.sorter.EarlyTerminatingSortingCollector;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.DefaultMergeSortFactory;

import java.util.Collection;
import java.util.Iterator;


/**
 * A factory interface for configuring a {@link Sort} in the Solr
 * schema.xml.
 */
public abstract class SortFactory {
  public static final String CLASS_NAME = "class";

  protected SolrParams params;

  public void init(SolrParams params, final Collection<SchemaField> required_fields) { this.params = params; }
  public SolrParams getParams() { return params; }

  /**
   * Returns {@link Sort} provided by the factory.
   *
   * Called by the {@link DefaultMergeSortFactory} to build a {@link SortingMergePolicy}.
   */
  public abstract Sort getSort();

  /**
   * Returns {@link Sort} if it is compatible with
   * the provided {@link Sort} object, returns null otherwise.
   *
   * Called by the {@link SolrIndexSearcher} to build an {@link EarlyTerminatingSortingCollector}.
   *
   * @param sort
   *          the non-null sort used for sorting the search results
   */
  public abstract Sort getSort(Sort sort);


  /** Returns a serializable description of this sort(factory) */
  public SimpleOrderedMap<Object> getNamedPropertyValues() {
    SimpleOrderedMap<Object> props = new SimpleOrderedMap<Object>();
    props.add(CLASS_NAME, getClassArg());
    if (null != params) {
      Iterator<String> iter = params.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        if ( ! CLASS_NAME.equals(key)) {
          props.add(key, params.get(key));
        }
      }
    }
    return props;
  }

  /**
   * @return the string used to specify the concrete class name in a serialized representation: the class arg.
   *         If the concrete class name was not specified via a class arg, returns {@code getClass().getName()},
   *         unless this class is the anonymous sort wrapper produced in {@link IndexSchema}, in which
   *         case the {@code getSort().getClass().getName()} is returned.
   */
  public String getClassArg() {
    if (null != params) {
      String className = params.get(CLASS_NAME);
      if (null != className) {
        return className;
      }
    }
    String className = getClass().getName();
    if (className.startsWith("org.apache.solr.schema.IndexSchema$")) {
      // If this class is just a no-params wrapper around a sort class, use the sort class
      className = getSort().getClass().getName();
    }
    return className;
  }
}
