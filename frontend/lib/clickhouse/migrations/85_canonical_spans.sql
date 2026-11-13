-- Delivery attempts remain in the physical append-only table. Every logical
-- span reader deduplicates by the scoped operation identity before filtering
-- or aggregating. Selecting the whole row keeps content and PII masks paired.
-- Prefer screened rows and the original trace-new content markers: a producer
-- retry can have empty new-message indices after its first delivery was seen.
CREATE VIEW IF NOT EXISTS spans_canonical SQL SECURITY INVOKER AS
SELECT *
FROM default.spans
QUALIFY row_number() OVER (
    PARTITION BY project_id, trace_id, span_id
    ORDER BY pii_checked DESC,
        length(input_new_message_indices) + length(output_new_message_indices) DESC,
        end_time DESC
) = 1;

DROP VIEW IF EXISTS spans_v0;
CREATE VIEW IF NOT EXISTS spans_v0 SQL SECURITY INVOKER AS
    WITH if(session_id != '', session_id, toString(trace_id)) AS dedup_group
    SELECT
        span_id,
        name,
        multiIf(
            span_kind = 0, 'DEFAULT',
            span_kind = 1, 'LLM',
            span_kind = 3, 'EXECUTOR',
            span_kind = 4, 'EVALUATOR',
            span_kind = 5, 'EVALUATION',
            span_kind = 6, 'TOOL',
            span_kind = 7, 'HUMAN_EVALUATOR',
            span_kind = 8, 'CACHED',
            'UNKNOWN'
        ) AS span_type,
        start_time,
        end_time,
        end_time - start_time AS duration,
        input_cost,
        output_cost,
        total_cost,
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
        reasoning_tokens,
        request_model,
        response_model,
        model,
        trace_id,
        provider,
        path,
        if(
            notEmpty(input_message_hashes),
            '[' || arrayStringConcat(
                arrayMap(
                    h -> if(
                        isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, h))),
                        dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, h), 'null'),
                        dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, h), 'null')
                    ),
                    input_message_hashes
                ),
                ','
            ) || ']',
            input
        ) AS input,
        if(
            notEmpty(output_message_hashes),
            '[' || arrayStringConcat(
                arrayMap(
                    h -> if(
                        isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, h))),
                        dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, h), 'null'),
                        dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, h), 'null')
                    ),
                    output_message_hashes
                ),
                ','
            ) || ']',
            output
        ) AS output,
        if(
            tool_definitions_hash != toFixedString('', 32),
            if(
                isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, tool_definitions_hash))),
                dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, tool_definitions_hash), ''),
                dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, tool_definitions_hash), '')
            ),
            ''
        ) AS tool_definitions,
        multiIf(status = 'error', 'error', status = 'success', 'success', 'success') AS status,
        parent_span_id,
        attributes,
        tags_array AS tags,
        events
    FROM spans_canonical
    WHERE project_id = {project_id:UUID};

DROP VIEW IF EXISTS spans_v1;
CREATE VIEW IF NOT EXISTS spans_v1 SQL SECURITY INVOKER AS
    WITH
        if(session_id != '', session_id, toString(trace_id)) AS dedup_group,
        dictGetOrDefault(
            'trace_access_policy_dict',
            ('user_id', 'metadata'),
            (project_id, trace_id),
            ('', '')
        ) AS trace_scope
    SELECT
        span_id,
        name,
        multiIf(
            span_kind = 0, 'DEFAULT',
            span_kind = 1, 'LLM',
            span_kind = 3, 'EXECUTOR',
            span_kind = 4, 'EVALUATOR',
            span_kind = 5, 'EVALUATION',
            span_kind = 6, 'TOOL',
            span_kind = 7, 'HUMAN_EVALUATOR',
            span_kind = 8, 'CACHED',
            'UNKNOWN'
        ) AS span_type,
        start_time,
        end_time,
        end_time - start_time AS duration,
        input_cost,
        output_cost,
        total_cost,
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
        reasoning_tokens,
        request_model,
        response_model,
        model,
        trace_id,
        provider,
        path,
        if(
            JSONExtractBool({policy:String}, 'maskPii'),
            if(
                notEmpty(input_message_hashes),
                '[' || arrayStringConcat(
                    arrayMap(
                        t -> if(
                            tupleElement(t, 3),
                            apply_pii_masks(tupleElement(t, 1), tupleElement(t, 2)),
                            '"[PII_MASKED_UNAVAILABLE]"'
                        ),
                        arrayMap(
                            h -> dictGetOrDefault(
                                'unique_content_dict',
                                ('content', 'content_masks', 'pii_checked'),
                                tuple(project_id, dedup_group, h),
                                ('', [], false)
                            ),
                            input_message_hashes
                        )
                    ),
                    ','
                ) || ']',
                if(pii_checked, apply_pii_masks(input, input_masks), '"[PII_MASKED_UNAVAILABLE]"')
            ),
            if(
                notEmpty(input_message_hashes),
                '[' || arrayStringConcat(
                    arrayMap(
                        h -> if(
                            isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, h))),
                            dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, h), 'null'),
                            dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, h), 'null')
                        ),
                        input_message_hashes
                    ),
                    ','
                ) || ']',
                input
            )
        ) AS input,
        if(
            JSONExtractBool({policy:String}, 'maskPii'),
            if(
                notEmpty(output_message_hashes),
                '[' || arrayStringConcat(
                    arrayMap(
                        t -> if(
                            tupleElement(t, 3),
                            apply_pii_masks(tupleElement(t, 1), tupleElement(t, 2)),
                            '"[PII_MASKED_UNAVAILABLE]"'
                        ),
                        arrayMap(
                            h -> dictGetOrDefault(
                                'unique_content_dict',
                                ('content', 'content_masks', 'pii_checked'),
                                tuple(project_id, dedup_group, h),
                                ('', [], false)
                            ),
                            output_message_hashes
                        )
                    ),
                    ','
                ) || ']',
                if(pii_checked, apply_pii_masks(output, output_masks), '"[PII_MASKED_UNAVAILABLE]"')
            ),
            if(
                notEmpty(output_message_hashes),
                '[' || arrayStringConcat(
                    arrayMap(
                        h -> if(
                            isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, h))),
                            dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, h), 'null'),
                            dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, h), 'null')
                        ),
                        output_message_hashes
                    ),
                    ','
                ) || ']',
                output
            )
        ) AS output,
        if(
            tool_definitions_hash != toFixedString('', 32),
            if(
                JSONExtractBool({policy:String}, 'maskPii'),
                -- Single-element arrayMap so the dict tuple is fetched once.
                arrayMap(
                    t -> if(
                        tupleElement(t, 3),
                        apply_pii_masks(tupleElement(t, 1), tupleElement(t, 2)),
                        '"[PII_MASKED_UNAVAILABLE]"'
                    ),
                    [dictGetOrDefault(
                        'unique_content_dict',
                        ('content', 'content_masks', 'pii_checked'),
                        tuple(project_id, dedup_group, tool_definitions_hash),
                        ('', [], false)
                    )]
                )[1],
                if(
                    isNull(dictGetOrNull('unique_content_dict', 'content', tuple(project_id, dedup_group, tool_definitions_hash))),
                    dictGetOrDefault('deduped_content_dict', 'content', tuple(project_id, tool_definitions_hash), ''),
                    dictGetOrDefault('unique_content_dict', 'content', tuple(project_id, dedup_group, tool_definitions_hash), '')
                )
            ),
            ''
        ) AS tool_definitions,
        multiIf(status = 'error', 'error', status = 'success', 'success', 'success') AS status,
        parent_span_id,
        attributes,
        tags_array AS tags,
        events
    FROM spans_canonical
    WHERE project_id = {project_id:UUID}
        AND trace_visible(tupleElement(trace_scope, 1), tupleElement(trace_scope, 2), {policy:String});

DROP VIEW IF EXISTS evaluation_datapoints_v0;
CREATE VIEW IF NOT EXISTS evaluation_datapoints_v0
SQL SECURITY INVOKER
AS SELECT
    edp.id id,
    edp.evaluation_id evaluation_id,
    dictGetOrDefault('evaluations_dict', 'name', (edp.project_id, edp.evaluation_id), '') evaluation_name,
    edp.data data,
    edp.target target,
    edp.metadata metadata,
    edp.executor_output executor_output,
    edp.index `index`,
    edp.task_id task_id,
    edp.trace_id trace_id,
    -- Postgres is the source of truth; the ClickHouse copy covers the dict's reload lag.
    dictGetOrDefault('evaluations_dict', 'group_id', (edp.project_id, edp.evaluation_id), edp.group_id) group_id,
    edp.scores scores,
    edp.updated_at updated_at,
    edp.created_at created_at,
    edp.dataset_id dataset_id,
    edp.dataset_datapoint_id dataset_datapoint_id,
    edp.dataset_datapoint_created_at dataset_datapoint_created_at,
    t.end_time - t.start_time duration,
    t.input_cost input_cost,
    t.output_cost output_cost,
    t.total_cost total_cost,
    t.start_time start_time,
    t.end_time end_time,
    t.input_tokens input_tokens,
    t.output_tokens output_tokens,
    t.total_tokens total_tokens,
    -- No status-bearing span means 'success', as in traces_v0; toLowCardinality
    -- preserves the previous column type.
    toLowCardinality(if(has(t.statuses, 'error'), 'error', 'success')) trace_status,
    ifNull(ts.metadata, '') trace_metadata,
    t.tags trace_tags,
    ifNull(ts.root_span_id, toUUID('00000000-0000-0000-0000-000000000000')) top_span_id,
    s.spans trace_spans
FROM evaluation_datapoints edp FINAL
LEFT JOIN
(
    SELECT
        project_id,
        id,
        -- Cast off the SimpleAggregateFunction wrapper so the exposed types
        -- stay plain DateTime64 as before
        CAST(min(start_time) AS DateTime64(9, 'UTC')) AS start_time,
        CAST(max(end_time) AS DateTime64(9, 'UTC')) AS end_time,
        sum(input_tokens) AS input_tokens,
        sum(output_tokens) AS output_tokens,
        sum(total_tokens) AS total_tokens,
        sum(input_cost) AS input_cost,
        sum(output_cost) AS output_cost,
        sum(total_cost) AS total_cost,
        groupUniqArrayArray(statuses) AS statuses,
        groupUniqArrayArray(tags) AS tags
    FROM
    (
        -- Nested scan because a bare `start_time` in the outer WHERE would bind
        -- to the `min(start_time)` alias above and fail as an aggregate in WHERE
        SELECT
            project_id,
            id,
            start_time,
            end_time,
            input_tokens,
            output_tokens,
            total_tokens,
            input_cost,
            output_cost,
            total_cost,
            statuses,
            tags
        FROM default.traces_agg
        WHERE project_id = {project_id:UUID}
            AND start_time >= {min_start_time:DateTime64(9)}
            AND start_time <= {max_start_time:DateTime64(9)}
            AND id IN (
                SELECT trace_id
                FROM default.evaluation_datapoints
                WHERE project_id = {project_id:UUID}
                    AND (empty({eval_ids:Array(UUID)})
                        OR evaluation_id IN {eval_ids:Array(UUID)})
            )
    )
    GROUP BY project_id, id
) AS t ON (t.project_id = edp.project_id) AND (t.id = edp.trace_id)
LEFT JOIN
(
    SELECT project_id, trace_id, metadata, root_span_id
    FROM default.traces_static FINAL
    PREWHERE project_id = {project_id:UUID}
        AND start_time >= {min_start_time:DateTime64(9)}
        AND start_time <= {max_start_time:DateTime64(9)}
        AND trace_id IN (
            SELECT trace_id
            FROM default.evaluation_datapoints
            WHERE project_id = {project_id:UUID}
                AND (empty({eval_ids:Array(UUID)})
                    OR evaluation_id IN {eval_ids:Array(UUID)})
        )
) AS ts ON (ts.project_id = edp.project_id) AND (ts.trace_id = edp.trace_id)
LEFT JOIN
(
    SELECT
        trace_id,
        project_id,
        groupArray(
            CAST(
                tuple(
                    name,
                    (toUnixTimestamp64Nano(end_time) - toUnixTimestamp64Nano(start_time)) / 1000000000,
                    span_type
                )
                AS
                Tuple(name String, duration Float64, type String)
            )
        ) AS spans
    FROM default.spans_canonical
    WHERE project_id = {project_id:UUID}
        AND start_time >= {min_start_time:DateTime64(9)}
        AND start_time <= {max_start_time:DateTime64(9)}
        AND trace_id IN (
            SELECT trace_id
            FROM default.evaluation_datapoints
            WHERE project_id = {project_id:UUID}
                AND (empty({eval_ids:Array(UUID)})
                    OR evaluation_id IN {eval_ids:Array(UUID)})
        )
    GROUP BY project_id, trace_id
) AS s ON (s.project_id = edp.project_id) AND (s.trace_id = edp.trace_id)
WHERE edp.project_id = {project_id:UUID}
    AND (empty({eval_ids:Array(UUID)}) OR edp.evaluation_id IN {eval_ids:Array(UUID)});
