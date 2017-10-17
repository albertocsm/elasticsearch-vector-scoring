/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mlnick.elasticsearch.script;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.script.ScriptException;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.TermPosition;

/**
 * Script that scores documents based on term vector payloads. Dot product and cosine similarity
 * is supported.
 */
public class PayloadVectorScoreScript extends AbstractSearchScript {

    // the field containing the vectors to be scored against
    String field = null;
    // indices for the query vector
    List<String> index = null;
    // vector for the query vector
    List<Double> vector = null;
    // whether to score cosine similarity (true) or dot product (false)
    boolean cosine = false;
    double queryVectorNorm = 0;

    final static public String SCRIPT_NAME = "payload_vector_score";

    /**
     * Factory that is registered in
     * {@link com.github.mlnick.elasticsearch.plugin.VectorScoringPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        /**
         * This method is called for every search on every shard.
         * 
         * @param params
         *            list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new PayloadVectorScoreScript(params);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        @Override
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * @param params index that a scored are placed in this parameter. Initialize them here.
     */
    @SuppressWarnings("unchecked")
    private PayloadVectorScoreScript(Map<String, Object> params) throws ScriptException {
        params.entrySet();
        // get field to score
        field = (String) params.get("field");
        // get query vector
        vector = (List<Double>) params.get("vector");
        // cosine flag
        Object cosineParam = params.get("cosine");
        if (cosineParam != null) {
            cosine = (boolean) cosineParam;
        }
        if (field == null || vector == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or vector parameter missing!");
        }
        // init index
        index = new ArrayList<>(vector.size());
        for (int i = 0; i < vector.size(); i++) {
            index.add(String.valueOf(i));
        }
        if (vector.size() != index.size()) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": index and vector array must have same length!");
        }
        if (cosine) {
            float average = 0f;
            for (double v : vector) {
                average += v;
            }

            average /= vector.size();

            // compute query vector norm once
            for (double v: vector) {
                queryVectorNorm += Math.pow(v - average, 2.0);
            }
        }
    }

    @Override
    public Object run() {

        float dotProduct = 0;
        double docVectorNorm = 0.0f;

        IndexField indexField = this.indexLookup().get(field);

        float average = 0f;
        for (String value : index) {
            IndexFieldTerm indexTermField = indexField.get(value, IndexLookup.FLAG_PAYLOADS);
            if (indexTermField != null) {
                Iterator<TermPosition> iter = indexTermField.iterator();
                if (iter.hasNext()) {
                    average += iter.next().payloadAsFloat(0f);
                }
            }
        }

        average /= index.size();

        // first, get the ShardTerms object for the field.
        for (int i = 0; i < index.size(); i++) {

            // get the vector value stored in the term payload
            IndexFieldTerm indexTermField = indexField.get(index.get(i), IndexLookup.FLAG_PAYLOADS);
            float docVector = 0f;
            if (indexTermField != null) {
                Iterator<TermPosition> iter = indexTermField.iterator();
                if (iter.hasNext()) {

                    docVector = iter.next().payloadAsFloat(0f) - average;
                    docVectorNorm += Math.pow(docVector, 2.0);
                }
            }
            // dot product
            dotProduct += docVector * vector.get(i);
        }

        // cosine similarity score
        if (docVectorNorm == 0 || queryVectorNorm == 0) {

            return 0f;
        } else {

            return dotProduct / (Math.sqrt(docVectorNorm) * Math.sqrt(queryVectorNorm));
        }
    }

}