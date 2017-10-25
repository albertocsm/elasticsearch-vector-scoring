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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * Script that scores documents based on term vector payloads. Dot product and cosine similarity
 * is supported.
 */
public class PayloadVectorScoreScriptTest extends AbstractSearchScriptTestCase {

    public static final ObjectMapper jsonMapper;
    private static final InputStream PRODUCT_MAPPING = ElasticsearchClient.class.getResourceAsStream("/product_mapping.json");
    private static final InputStream INDEX_SETTINGS = ElasticsearchClient.class.getResourceAsStream("/settings.json");
    private static final InputStream PRODUCT_EXAMPLE = ElasticsearchClient.class.getResourceAsStream("/example_product.json");
    private static final InputStream QUERY_FEATURES_EXAMPLE = ElasticsearchClient.class.getResourceAsStream("/query_feature_vector.json");
    public static final String INDEX = "products";
    public static final String PRODUCT_TYPE = "products";

    static {
        jsonMapper = new ObjectMapper();
        jsonMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .build();
    }

    @Test
    public void testPayloadVectorScoreScript() throws Exception {


        // prepare artifacts for testing
        String settingsSource = CharStreams.toString(new InputStreamReader(INDEX_SETTINGS, "UTF-8"));
        String productMappingSource = CharStreams.toString(new InputStreamReader(PRODUCT_MAPPING, "UTF-8"));
        String product = CharStreams.toString(new InputStreamReader(PRODUCT_EXAMPLE, "UTF-8"));
        List<Double> listFeatures = jsonMapper.readValue(
            new InputStreamReader(QUERY_FEATURES_EXAMPLE, "UTF-8"),
            new TypeReference<List<Double>>() {
            });

        // ingest data into ES
        assertAcked(prepareCreate(INDEX).setSettings(settingsSource).addMapping(PRODUCT_TYPE, productMappingSource));
        index(INDEX, PRODUCT_TYPE, UUID.randomUUID().toString(), product);


        MatchAllQueryBuilder criteria = matchAllQuery();
        Map<String, Object> params = new HashMap<>();
        params.put("field", "features");
        params.put("vector", listFeatures);
        params.put("cosine", true);
        QueryBuilder qb = functionScoreQuery(
            criteria,
            ScoreFunctionBuilders.scriptFunction(
                new Script(
                    PayloadVectorScoreScript.SCRIPT_NAME,
                    ScriptService.ScriptType.INLINE,
                    "native",
                    params))
        ).boostMode(CombineFunction.REPLACE);

        Thread.sleep(5000);

        SearchResponse searchResponse = client().prepareSearch(INDEX)
            .setQuery(qb)
            .setTypes(PRODUCT_TYPE)
            .setSize(10)
            .execute()
            .actionGet();

        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

    }
}