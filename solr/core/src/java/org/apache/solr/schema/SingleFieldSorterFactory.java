package org.apache.solr.schema;

/*
 * Modelled on DefaultSimilarityFactory class.
 */

import org.apache.lucene.index.sorter.NumericDocValuesSorter;
import org.apache.lucene.index.sorter.Sorter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.params.SolrParams;

import java.util.Collection;

/**
 * Factory for a {@link Sorter} on a single {@link SchemaField}
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
public class SingleFieldSorterFactory extends SorterFactory {
  protected String fieldName;
  protected Boolean ascending;
  protected SortField sort_field;
  protected Sorter sorter;
  
  @Override
  public void init(SolrParams params, final Collection<SchemaField> required_fields) {
    super.init(params, required_fields);
    
    fieldName = params.required().get("fieldName");
    ascending = params.required().getBool("ascending");
    sort_field = null;
    sorter = null;
    
    for (SchemaField field : required_fields) {
      if (!field.getName().equals(fieldName)) {
        continue;
      }
      else if (field.hasDocValues()) {
        // TODO - check that field has _numeric_ doc values
        sort_field = field.getSortField(!ascending /* top/reverse */);
        sorter = new NumericDocValuesSorter(fieldName, ascending);
        break;
      }
      // TODO - add support for others i.e. non-numeric and/or non-docvalue
    }
  }
  
  /**
   * Returns {@link Sorter} for a single field.
   */
  @Override
  public Sorter getSorter() {
    return sorter;
  }
  
  /**
   * Returns {@link Sorter} for a single field if it is compatible with
   * the provided {@link Sort} object, returns null otherwise.
   * 
   * Examples:
   *   Sorter(name,ascending) is compatible with Sort([name,ascending])
   *   Sorter(name,ascending) is not compatible with Sort([name,descending])
   *   Sorter(name,ascending) is not compatible with Sort([description,ascending])
   *   Sorter(name,ascending) is not compatible with Sort([description,ascending], [name,ascending])
   *   Sorter(name,ascending) is compatible with Sort([name,ascending], [description,descending])
   * 
   * @param sort
   *          the non-null sort used for sorting the search results
   */
  @Override
  public Sorter getSorter(Sort sort) {
    SortField[] fields = sort.getSort();
    if (0 < fields.length && fields[0].equals(sort_field)) {
      return sorter;
    }
    return null;
  }
}
