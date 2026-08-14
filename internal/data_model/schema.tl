// Mega TODO - when all statshouses updated to commit containing this message, we can:
// 1. remove all functions without "2" suffix from this TL, statshouse.sourceBucket, statshouse.item, statshouse.ingestion_status, statshouse.string_top_element
// 2. remove code referring to them from aggregator (recursively)
// This commit is made on 2022-sep-25

// Protocol between statshouse and statshouse aggregator is private for now

---types---

statshouse.sampleFactor {fields_mask:#} metric:int value:float original_size:fields_mask.0?# = statshouse.SampleFactor fields_mask;
statshouse.metricBudget metric_id:int budget:# = statshouse.MetricBudget;

statshouse.ingestionStatus2 env:int metric:int value:float = statshouse.IngestionStatus2;

statshouse.centroidFloat#73fd01e0 value:float count:float = statshouse.CentroidFloat;

statshouse.mapping str:string value:int = statshouse.Mapping;

statshouse.multiValue#0c803e06 {fields_mask:#}
    counter: fields_mask.0?double // no counter means counter equal to 0
    counter_eq_1: fields_mask.1?true // optimization for most common value

    value_set: fields_mask.2?true // to avoid min/max corruption when some clients treat the same row as counter/value

    value_min: fields_mask.3?double // simple value (all values identical)
    value_max: fields_mask.4?double // complex value (there is different values)
    value_sum: fields_mask.4?double // complex value (there is different values)
    value_sum_square: fields_mask.4?double // complex value (there is different values)

    uniques: fields_mask.5?string // serialized HLL

    centroids: fields_mask.6?(vector statshouse.centroidFloat)
    implicit_centroid: fields_mask.18?true // centroid should be restored from the single simple value

    // we need hosts to pass through agent sending on behalf of aggregator. Or from clients using _h tag
    max_host_tag: fields_mask.7?int
    // optimization for single values: if max_host_tag is set and *_host_tag below is not set, it is equal to max_host_tag
    min_host_tag: fields_mask.8?int
    max_counter_host_tag: fields_mask.9?int
    // field mask bits 10 - 12 are used in statshouse.multiItem
    max_host_stag: fields_mask.14?string
    min_host_stag: fields_mask.15?string
    max_counter_host_stag: fields_mask.16?string
    = statshouse.MultiValue fields_mask;

statshouse.topElement#9ffdea42 stag:string fields_mask:# tag:fields_mask.10?int value:(statshouse.multiValue fields_mask) = statshouse.TopElement;

statshouse.multiItem#0c803e07
    fields_mask: # // conveyor V2 agents send shard num in top 8 bits here
    metric: int
    keys: (vector int)
    skeys: fields_mask.12?(vector string)
    weightMultiplier: fields_mask.17?true // agents increased metric weight by num shards, expect the same from aggregators

    t: fields_mask.10?# // timestamp, should be rare

    tail: (statshouse.multiValue fields_mask) // no fields_mask, because anyway uses 0 bytes if counter with 0.0 or 1.0
    // won't be set in a v3 pipeline
    top: fields_mask.11?(vector (statshouse.topElement)) // most items are without string keys, so we save 4 bytes using fields_mask
    = statshouse.MultiItem;

statshouse.sourceBucket3#16c4dd7b
    fields_mask: #
    // TODO: check how it is used and maybe change to a new version
    have_original_size: fields_mask.0?true
    metrics: vector<statshouse.multiItem> // must be sorted by metric (but for now order is not used)
    sample_factors: vector<statshouse.sampleFactor<fields_mask>> // We need as compact representation as possible.
    ingestion_status_ok2: vector<statshouse.ingestionStatus2> // We need as compact representation as possible.
  = statshouse.SourceBucket3;

// TODO - remove ttl_nanosec
statshouse.getTagMappingResult#1a7d91fd value:int ttl_nanosec:long = statshouse.GetTagMappingResult;

statshouse.getTagMappingBootstrapResult#486a40de mappings:(vector statshouse.mapping) = statshouse.GetTagMappingBootstrapResult;
statshouse.putTagMappingBootstrapResult#486affde
    count_inserted: int
 = statshouse.PutTagMappingBootstrapResult;

// metrics are row in JSON format, version is opaque string interpreted by aggregator
statshouse.getMetricsResult#0c803d05 version: string metrics: (vector string) = statshouse.GetMetricsResult;

// Deprecated, not used by new conveyor agents
statshouse.getConfigResult#0c803d07 {fields_mask:#}
    addresses: (vector string)
    max_addresses_count: int // when reducing # of shards, will return full list of addresses, limit by this number. Important for proxy
    previous_addresses: int // currently unused, but can be helpful for proxy in the future
    ts: fields_mask.0?long // aggregator timestamp, for detecting clock discrepancy
= statshouse.GetConfigResult fields_mask;

statshouse.getConfigResult3#f13698cb
    addresses: (vector string) // 3 replicas for each shard
    shard_by_metric_count: # // should be <= len(addresses), so that we can add 17'th shard without breaking all metrics which are spread automatically between first 16.
    fields_mask: #
    ingress_proxy: fields_mask.0?true
    agent_ip: 4*[int] // as visible by server during getConfig

    unused: 7*[int] // arbitrary space for extensions, so we can avoid using fields masks and complications with them

    connected_to: string // hostname or other ID of ingress proxy or aggregator

    unused_s: 3*[string] // arbitrary space for extensions, so we can avoid using fields masks and complications with them
= statshouse.GetConfigResult3;

// proxy uses and fills data in this header for all requests
statshouse.commonProxyHeader#6c803d07 {fields_mask:#}
    // header uses bits backward to reduce conflicts with request fields
    ingress_proxy:fields_mask.31?true     // simply set to 1 by ingress proxy for all passing requests

    // bits set by agents according to statshouse_env arg to help with debug by separating built-in metrics from different sets of agents
    agent_env_staging_0:fields_mask.30?true
    agent_env_staging_1:fields_mask.29?true

    shard_replica:int // Aggregator will only accept correct shard/replica conbinations
    shard_replica_total:int
    agent_ip:4*[int] // filled by proxy. ipv4 is in ipv6-compatible format, source_ip[3] holds ipv4 address bits.
    host_name:string
    component_tag:int // was agent_env before release version. Checks for legacy in aggregator are no more needed after release
    build_arch:int // comparison of x64 and arm64 nodes will be interesting for some time
    owner:fields_mask.28?string
= statshouse.CommonProxyHeader fields_mask;

statshouse.promTarget#ac5296df {fields_mask_arg:#}
    fields_mask: #
    job_name: string
    url: string
    labels: (dictionary string)
    scrape_interval: long
    honor_timestamps: fields_mask.0?true
    honor_labels: fields_mask.1?true
    scrape_timeout: long
    body_size_limit: long
    label_limit: long
    label_name_length_limit: long
    label_value_length_limit: long
    // raw yaml from prom config
    http_client_config: string
    metric_relabel_configs: fields_mask_arg.1?string
= statshouse.PromTarget fields_mask_arg;

statshouse.getTargetsResult#51ac86df {fields_mask:#}
    targets: (vector (statshouse.promTarget fields_mask))
    hash: string
    gauge_metrics:fields_mask.0?(vector string)
= statshouse.GetTargetsResult fields_mask;

// saved between agent invocations. All values nanoseconds.
statshouse.shutdownInfo#4124cf9c
    startShutdownTime:long
    finishShutdownTime:long
    stopRecentSenders:long
    stopReceivers:long
    stopFlusher:long
    stopFlushing:long
    stopPreprocessor:long
    stopInserters:long // for aggregator
    stopRPCServer:long // for aggregator
    saveMappings:long
    saveJournal:long
    // we might want more timeouts later without changing format, so we reserve some space
    c:long d:long e:long f:long g:long h:long i:long j:long k:long l:long m:long n:long
= statshouse.ShutdownInfo;

statshouse.sendSourceBucket3Response#0e177acc
    field_mask: #
    discard: field_mask.0?true // agent should discard bucket, either they were inserted successfully or they can never be inserted
    warning: string // if !empty, aggregator wants this in client log
    mappings: (vector statshouse.mapping)
    metric_budgets: field_mask.1?(vector statshouse.metricBudget)
= statshouse.SendSourceBucket3Response;

// ---------------------------------------------------------------- store query RPC
//
// Structured query protocol between statshouse-api and the aggregator's storage
// backend (duck-store). Served by the aggregator on a separate listener from
// ingest. Backend-neutral naming: nothing here is DuckDB-specific, and SQL text
// never crosses this wire. See .scratch/duck-store/04-query-rpc.tl for the
// resolved contract.

// One tag predicate. Within a filter the mapped/values/empty/re2 arms are OR-ed
// for an IN filter and AND-ed for a NOT IN filter (what filterOperatorIn/NotIn do
// in internal/api/sql_query_series.go). re2 is unanchored RE2; values must bind
// as prepared statement parameters, never interpolate into SQL text.
statshouse.storeTagFilter#57cb1001
    fields_mask:#
    tag_index:int                          // 0 .. format.MaxTags-1 (48)
    mapped:fields_mask.0?(vector long)     // long, not int: carries a raw64 value whole
    values:fields_mask.1?(vector string)   // unmapped string values, matched against stagN
    empty:fields_mask.2?true               // tagN = 0 AND stagN = ''
    re2:fields_mask.3?string
    = statshouse.StoreTagFilter;

// Per-tag storage interpretation as the API resolved it from ITS journal:
// 0 = mapped  tagN holds a mapping id, stagN may hold an unmapped string
// 1 = raw32   tagN holds the value directly, stagN unused
// 2 = raw64   value SPLIT: low half in tagN, high half in tagN+1
// The aggregator validates against its own journal at metric_version and
// returns metadata_mismatch on disagreement — it never reinterprets rows.
statshouse.storeTagLayout#57cb1002
    kinds:(vector int)
    = statshouse.StoreTagLayout;

// The queried resolution window. step_sec is one of the 11 LOD steps; the
// aggregator maps it onto the 1s/1m/1h tier table itself. location is only
// meaningful for the _1M (calendar month) step; every other step uses utc_offset.
statshouse.storeLod#57cb1003
    from_sec:long        // inclusive
    to_sec:long          // exclusive
    step_sec:long
    utc_offset:long
    location:string      // IANA name, e.g. "Europe/Moscow"
    = statshouse.StoreLod;

// Shared by both query verbs.
statshouse.storeQueryBase#57cb1004
    fields_mask:#
    metric_id:int
    metric_version:long                             // journal version tag_layout was read at
    tag_layout:statshouse.StoreTagLayout
    metric_in:fields_mask.0?(vector int)            // multi-metric filter (PromQL)
    metric_not_in:fields_mask.1?(vector int)
    lod:statshouse.StoreLod
    filter_in:(vector statshouse.StoreTagFilter)
    filter_not_in:(vector statshouse.StoreTagFilter)
    row_limit:int                                   // clamped by the aggregator to its own max
    timeout_ms:int                                  // RELATIVE, not an absolute deadline: immune to clock skew
    = statshouse.StoreQueryBase;

// One batch of columnar results; columns are parallel arrays of length `rows`.
// Forward-compatibility hinge: a later streaming verb returns the SAME type one
// batch at a time. Aggregate states ride as unmodified ClickHouse state bytes,
// never finalized — a per-shard percentile is meaningless before the API merges
// across shards and LODs. The aggregator has already folded them in Go.
// Host columns are plain (int, string) tag pairs merged by a single
// arg_min/arg_max over a struct_pack, so they ship structurally and are compared
// by value, not by bytes.
statshouse.storeSeriesBatch#57cb1005
    fields_mask:#
    rows:int
    time:(vector long)                              // start of each bucket, unix seconds
    tag:(vector %(vector long))                     // one vector per grouped tag, in `by` order
    stag:(vector %(vector string))                  // parallel to `tag`; empty vector if raw
    count:fields_mask.0?(vector double)
    min:fields_mask.1?(vector double)
    max:fields_mask.2?(vector double)
    sum:fields_mask.3?(vector double)
    sumsquare:fields_mask.4?(vector double)
    cardinality:fields_mask.5?(vector double)
    percentiles:fields_mask.6?(vector string)       // CH quantilesTDigest state bytes
    uniq_state:fields_mask.7?(vector string)        // CH uniq state bytes
    min_host_value:fields_mask.8?(vector double)
    min_host_tag:fields_mask.8?(vector int)
    min_host_stag:fields_mask.8?(vector string)
    max_host_value:fields_mask.9?(vector double)
    max_host_tag:fields_mask.9?(vector int)
    max_host_stag:fields_mask.9?(vector string)
    = statshouse.StoreSeriesBatch;

statshouse.storeSeriesResponse#57cb1006
    fields_mask:#
    shard_num:int                       // the aggregator's own shard, supplied as a literal: nothing is stored per-row
    batches:(vector statshouse.StoreSeriesBatch)
    = statshouse.StoreSeriesResponse;

statshouse.storeTagValuesResponse#57cb1007
    fields_mask:#
    tag:(vector long)
    stag:(vector string)                // empty when ids_only or the tag is raw
    count:(vector double)
    = statshouse.StoreTagValuesResponse;

---functions---

// Deprecated, not used by new conveyor agents
@readwrite statshouse.getConfig2#4285ff57
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask) // the only request not passed through proxy. But we make it with common header anyway.
    cluster:string // prevent wrong configuration
    ts: fields_mask.0?true
    newIngressVersion: fields_mask.1?true // for ingress version without compat
     = statshouse.GetConfigResult fields_mask;

@readwrite statshouse.getConfig3#7d7b4991
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask) // the only request not passed through proxy. But we make it with common header anyway.
    cluster:string // prevent wrong configuration
    previousConfig: fields_mask.0?statshouse.getConfigResult3 // if sent, will long poll on it
    newIngressVersion: fields_mask.1?true // for ingress version without compat
     = statshouse.GetConfigResult3;

// For ingress proxy, we pass shard_replica/shard_total immediately after fields mask
// TODO - remove shard_replica, shard_replica_total fields from the wrong place

// sendSourceBucket and sendKeepAlive must share common response, because aggregator hijacks them into the same slice
// we use string as a crude logging capability. Agents write to log what they receive from aggregators.
// type of compressed_data should be checked by first 4 bytes (tag)
@readwrite statshouse.sendSourceBucket3#0d04aa3f
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    time:#
    historic:fields_mask.0?true
    spare:fields_mask.1?true
    // build stats
    build_commit:string
    build_commit_ts:#
    // data
    original_size:#
    compressed_data:string
    send_more_bytes:string // send dummy zeroes to test network, according to send-more-data agent config value
    = statshouse.SendSourceBucket3Response;

// agents with new conveyor still use this function.
// remove from aggregators after all agents are at least this version
@readwrite statshouse.sendKeepAlive2#4285ff53
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = String;

@readwrite statshouse.sendKeepAlive3#4285ff54
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = statshouse.SendSourceBucket3Response;

// This function is long poll. Will receive answer only when version changes
@readwrite statshouse.getMetrics3#42855554
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    field_mask: #
    from: long // строго больше
    limit: long
    return_if_empty: field_mask.3?true
    compact_journal: field_mask.4?true // will return from new compact journal (only fields for agents)
     = metadata.GetJournalResponsenew fields_mask;

@readwrite statshouse.getTagMapping2#4285ff56
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    metric:string
    key:string
    create:fields_mask.0?true // required, after initial error caches send request to create with empty name, we assign such requests to empty metric
    tag_id_key:int // to look for tag which is source of garbage
    client_env:int // to look for env which is source of garbage
     = statshouse.GetTagMappingResult;

@readwrite statshouse.autoCreate#28bea524
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    metric:string // metric name
    kind:string   // metric kind
    tags:(vector string) // metric tags
    resolution:fields_mask.0?int
    description:fields_mask.1?string
     = True;

// when local cache is empty, statshouse will ask aggregator for common mappings
@readwrite statshouse.getTagMappingBootstrap#75a7f68e
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
     = statshouse.GetTagMappingBootstrapResult;

@readwrite statshouse.getTargets2#41df72a3
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    prom_host_name:string // in contrast to normalized and validated for statshouse key, like in header
    old_hash: string
    gauge_metrics:fields_mask.0?Bool
    metric_relabel_configs:fields_mask.1?Bool
     = statshouse.GetTargetsResult fields_mask;

@readwrite statshouse.testConnection2#4285ff58
    fields_mask:#
    header: (statshouse.commonProxyHeader fields_mask)
    payload:string
    response_size:int // will reply with response of this size
    response_timeout_sec:int // if >0, will long poll for requested time
     = String;

// The series verb. Covers every read surface that goes through buildSeriesQuery
// today — series, point queries, the table view, cardinality, PromQL series and
// the 1/sec cache-invalidation log query (a tag-only series query: empty `what`,
// group by tag1). what[] carries data_model.DigestSelector kinds; an empty
// what[] is legal and means "grouped tags and timestamps only". by[] carries
// grouped tag indices; format.ShardTagIndex (-3) is legal and is answered from
// shard_num, not from storage.
//
// Row limit semantics: the aggregator executes with LIMIT row_limit+1, because
// LIMIT N cannot distinguish "exactly N rows" from truncation. If the extra row
// appears the call FAILS with row_limit — a partial series result is never
// returned and never cached by the API.
@read statshouse.storeQuerySeries#57cb1008
    fields_mask:#
    base:%statshouse.StoreQueryBase
    what:(vector int)
    by:(vector int)
    min_host:fields_mask.0?true
    max_host:fields_mask.1?true
    sort_desc:fields_mask.2?true                    // table view; absent = unsorted
    sort_asc:fields_mask.3?true
    = statshouse.StoreSeriesResponse;

// The tag-values verb, covering both the values and the ids-only mode.
//
// CRITICAL: the aggregator MUST NOT apply the user's top-N. A value ranked below
// N on every individual shard can still be globally top-N once counts are
// summed, so per-shard top-N silently returns wrong answers. The aggregator
// returns ALL values up to row_limit (a safety cap, not the user's N) and fails
// on truncation; the API sums counts across shards and takes the global top N.
@read statshouse.storeQueryTagValues#57cb1009
    fields_mask:#
    base:%statshouse.StoreQueryBase
    tag_index:int
    ids_only:fields_mask.0?true                     // ids-only mode
    = statshouse.StoreTagValuesResponse;
