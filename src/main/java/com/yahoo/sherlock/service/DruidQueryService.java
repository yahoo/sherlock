/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.query.QueryBuilder;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.query.Query;

import lombok.extern.slf4j.Slf4j;

/**
 * Service class to validate user query and generate new query with given granularity.
 */
@Slf4j
public class DruidQueryService {

    /**
     * Method to build new query from pre-computed interval end time.
     *
     * @param queryString input druid query from user
     * @param granularity input granularity value for query
     * @param granularityRange range of granularity to aggregate on
     * @param intervalEndTime interval end time of query
     * @return query object with new generated query
     * @throws SherlockException exception while parsing user query
     */
    public Query build(String queryString, Granularity granularity, Integer granularityRange, Integer intervalEndTime) throws SherlockException {
        return QueryBuilder.start()
            .endAt(intervalEndTime)
            .granularity(granularity)
            .granularityRange(granularityRange)
            .queryString(queryString)
            .build();
    }
}
