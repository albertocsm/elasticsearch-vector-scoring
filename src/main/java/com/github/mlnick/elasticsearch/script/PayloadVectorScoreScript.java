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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.TermPosition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Script that scores documents based on term vector payloads. Dot product and cosine similarity
 * is supported.
 */
public class PayloadVectorScoreScript extends AbstractSearchScript {

    // the field containing the vectors to be scored against
    String field = null;
    // indices for the query vector
    List<Integer> index = null;
    // vector for the query vector
    List<Double> queryVector = null;
    // whether to score cosine similarity (true) or dot product (false)
    boolean cosine = false;
    double queryVectorNorm = 0;
    float queryVectorAvg = 0;

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
        queryVector = (List<Double>) params.get("vector");

        // cosine flag
        Object cosineParam = params.get("cosine");
        if (cosineParam != null) {
            cosine = (boolean) cosineParam;
        }
        if (field == null || queryVector == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or vector parameter missing!");
        }
        // init index
        index = new ArrayList<>();
        for (int i = 0; i < queryVector.size(); i++) {
            if (queryVector.get(i) > 0) {
                index.add(i);
            }
        }

        float queryVectorTotal = 0f;
        for (Integer idx : index) {
            queryVectorTotal += queryVector.get(idx);
        }
        queryVectorAvg = queryVectorTotal / index.size();

        // compute query vector norm once
        for (Integer idx : index) {
            queryVectorNorm += Math.pow(queryVector.get(idx) - queryVectorAvg, 2.0);
        }
    }

    @Override
    public Object run() {

        // first, get the ShardTerms object for the field.
        IndexField indexField = this.indexLookup().get(field);

        List<Float> docVector = new ArrayList<>();
        for (Integer idx : index) {

            // get the vector value stored in the term payload
            IndexFieldTerm indexTermField = indexField.get(String.valueOf(idx), IndexLookup.FLAG_PAYLOADS);
            if (indexTermField != null) {
                Iterator<TermPosition> iter = indexTermField.iterator();
                if (iter.hasNext()) {
                    docVector.add(iter.next().payloadAsFloat(0f));
                }
            }
        }

        if (docVector.isEmpty()) {
            return 0f;
        }

        if (docVector.size() != index.size()) {
            return 0f;
        }

        float docVectorAvg = 0f;
        for (Float v: docVector){
            docVectorAvg += v;
        }
        docVectorAvg /= docVector.size();

        float docVectorValue;
        float dotProduct = 0;
        double docVectorNorm = 0.0f;
        for (int i = 0; i < docVector.size(); i++) {
            docVectorValue = docVector.get(i) - docVectorAvg;
            docVectorNorm += Math.pow(docVectorValue, 2.0);

            // dot product
            Integer queryVectorIdx = index.get(i);
            dotProduct += docVectorValue * (queryVector.get(queryVectorIdx) - queryVectorAvg);
        }

        // cosine similarity score
        if (docVectorNorm == 0 || queryVectorNorm == 0) {

            return 0f;
        } else {

            return dotProduct / (Math.sqrt(docVectorNorm) * Math.sqrt(queryVectorNorm));
        }
    }
}