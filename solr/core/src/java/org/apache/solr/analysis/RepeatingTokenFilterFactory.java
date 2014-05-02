package org.apache.solr.analysis;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

//analyzer filter factory for considering the last payload value as the term frequency
public class RepeatingTokenFilterFactory extends TokenFilterFactory {
    public static final String DELIMITER_ATTR = "delimiter";

    private final char delimiter;

    /** Creates a new RepeatingTokenFilterFactory */
    public RepeatingTokenFilterFactory(Map<String,String> args) {
      super(args);
      delimiter = getChar(args, DELIMITER_ATTR, ':');
      if (!args.isEmpty()) {
        throw new IllegalArgumentException("Unknown parameters: " + args);
      }
    }
    
    @Override
    public TokenStream create(TokenStream input) {
      return new RepeatingTokenFilter(input, delimiter);
    }
}
