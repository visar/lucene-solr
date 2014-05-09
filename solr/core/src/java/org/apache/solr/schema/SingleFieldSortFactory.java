package org.apache.solr.schema;

/*
 * Modelled on DefaultSimilarityFactory class.
 */

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.params.SolrParams;

import java.util.Collection;

/**
 * Factory for a {@link Sort} on a single {@link SchemaField}
 * <p>
 * The field must be a one of the {@link IndexSchema#getRequiredFields()} fields.
 * The field must have a {@link SchemaField#getSortField(boolean)} field.
 * <p>
 *
 * The current implementation is for numeric doc value fields only.
 *
 * Required settings:
 * <ul>
 *   <li>fieldName (String)</li>
 *   <li>ascending (boolean)</li>
 * </ul>
 */
public class SingleFieldSortFactory extends SortFactory {
  protected String fieldName;
  protected Boolean ascending;
  protected SortField sort_field;
  protected Sort sort;

  @Override
  public void init(SolrParams params, final Collection<SchemaField> required_fields) {
    super.init(params, required_fields);

    fieldName = params.required().get("fieldName");
    ascending = params.required().getBool("ascending");
    sort_field = null;
    sort = null;

    for (SchemaField field : required_fields) {
      if (!field.getName().equals(fieldName)) {
        continue;
      }
      else if (field.hasDocValues()) {
        sort_field = field.getSortField(!ascending /* top/reverse */);
        sort = new Sort(sort_field);
        break;
      }
    }
  }

  /**
   * Returns {@link Sort} for a single field.
   */
  @Override
  public Sort getSort() {
    return sort;
  }

  /**
   * Returns {@link Sort} for a single field if it is compatible with
   * the provided {@link Sort} object, returns null otherwise.
   *
   * Examples:
   *   Sort([name,ascending]) is compatible with Sort([name,ascending])
   *   Sort([name,ascending]) is not compatible with Sort([name,descending])
   *   Sort([name,ascending]) is not compatible with Sort([description,ascending])
   *   Sort([name,ascending]) is not compatible with Sort([description,ascending], [name,ascending])
   *   Sort([name,ascending]) is compatible with Sort([name,ascending], [description,descending])
   *
   * @param p_sort
   *          the non-null sort used for sorting the search results
   */
  @Override
  public Sort getSort(Sort p_sort) {
    SortField[] fields = p_sort.getSort();
    if (0 < fields.length && fields[0].equals(sort_field)) {
      return sort;
    }
    return null;
  }
}
