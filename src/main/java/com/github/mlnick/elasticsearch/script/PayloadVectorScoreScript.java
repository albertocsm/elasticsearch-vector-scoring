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

import org.apache.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Script that scores documents based on term vector payloads. Dot product and cosine similarity
 * is supported.
 */
public class PayloadVectorScoreScript extends AbstractSearchScript {

    private static final Logger log = Logger.getLogger(PayloadVectorScoreScript.class);
    public static final String SCRIPT_NAME = "payload_vector_score";

    private String field = null;
    private List<Double> queryVector = null;
    private boolean cosine = false;
    private double queryVectorNorm = 0;
    private float queryVectorAvg = 0;


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

        float queryVectorTotal = 0f;
        for (double v : queryVector) {
            queryVectorTotal += v;
        }
        queryVectorAvg = queryVectorTotal / queryVector.size();

        for (double v : queryVector) {
            queryVectorNorm += Math.pow(v - queryVectorAvg, 2.0);
        }

    }

    @Override
    public Object run() {

        String features = (String) source().get(field);
        if (features == null || features.isEmpty()) {

            log.warn("Document does not have features");
            return 0f;
        }

        List<Double> docVector = convertFeaturesToList(features);
        if (docVector == null || docVector.isEmpty()) {

            log.warn("Document does not have features");
            return 0f;
        }

        if (docVector.size() != queryVector.size()) {

            log.error("DocVector and Query vectors sizes are not the same!!!");
            return 0f;
        }

        Double docVectorAvg = 0D;
        for (Double value : docVector) {
            docVectorAvg += value;
        }
        docVectorAvg /= docVector.size();

        Double docVectorValue;
        Double dotProduct = 0D;
        Double docVectorNorm = 0.0D;
        for (int i = 0; i < queryVector.size(); i++) {
            docVectorValue = docVector.get(i) - docVectorAvg;
            docVectorNorm += Math.pow(docVectorValue, 2.0);

            // dot product
            dotProduct += docVectorValue * (queryVector.get(i) - queryVectorAvg);
        }

        // cosine similarity score
        if (docVectorNorm == 0 || queryVectorNorm == 0) {

            return 0f;
        } else {

            return dotProduct / (Math.sqrt(docVectorNorm) * Math.sqrt(queryVectorNorm));
        }
    }

    private List<Double> convertFeaturesToList(String features) {

        if (features == null || features.isEmpty()) {
            return null;
        }

        List<Double> list = new ArrayList<>();
        for (String out : features.split(" ")) {
            Double featureVal = Double.parseDouble(out.split("\\|")[1]);
            list.add(featureVal);
        }

        return list;
    }


}