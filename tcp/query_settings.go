package tcp

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"strconv"
	"strings"
)

type querySettingType int

// all possible query setting's data type
const (
	uintQS querySettingType = iota + 1
	intQS
	boolQS
	timeQS
)

// description of single query setting
type querySettingInfo struct {
	name   string
	qsType querySettingType
}

func (setting *querySettingInfo) getValue(decoder *binary.Decoder) (string, error) {
	switch setting.qsType {
	case uintQS, timeQS:
		value, err := decoder.Uvarint()
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(value, 10), nil
	case intQS:
		value, err := decoder.Int64()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(value, 10), nil
	case boolQS:
		val, err := decoder.Bool()
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(val), nil
	default:
		return "", fmt.Errorf("not support setting type")
	}
}

type settingsInfo struct {
}

func (setting *settingsInfo) deserialize(decoder *binary.Decoder) (map[string]string, error) {
	settings := make(map[string]string)
	for {
		name, err := decoder.String()
		if err != nil {
			return nil, fmt.Errorf("get query setting name error")
		}
		if name == "" {
			break
		}
		for i, info := range querySettingList {
			if (len(querySettingList) - 1) == i {
				return nil, fmt.Errorf("proxy not contains this setting name: %s", name)
			}
			if !strings.EqualFold(info.name, name) {
				continue
			}
			value, err := info.getValue(decoder)
			if err != nil {
				return nil, err
			}
			settings[name] = value
			break
		}
	}
	return settings, nil
}

// all possible query settings
var querySettingList = []querySettingInfo{
	{"min_compress_block_size", uintQS},
	{"max_compress_block_size", uintQS},
	{"max_block_size", uintQS},
	{"max_insert_block_size", uintQS},
	{"min_insert_block_size_rows", uintQS},
	{"min_insert_block_size_bytes", uintQS},
	{"max_read_buffer_size", uintQS},
	{"max_distributed_connections", uintQS},
	{"max_query_size", uintQS},
	{"interactive_delay", uintQS},
	{"poll_interval", uintQS},
	{"distributed_connections_pool_size", uintQS},
	{"connections_with_failover_max_tries", uintQS},
	{"background_pool_size", uintQS},
	{"background_schedule_pool_size", uintQS},
	{"replication_alter_partitions_sync", uintQS},
	{"replication_alter_columns_timeout", uintQS},
	{"min_count_to_compile", uintQS},
	{"min_count_to_compile_expression", uintQS},
	{"group_by_two_level_threshold", uintQS},
	{"group_by_two_level_threshold_bytes", uintQS},
	{"aggregation_memory_efficient_merge_threads", uintQS},
	{"max_parallel_replicas", uintQS},
	{"parallel_replicas_count", uintQS},
	{"parallel_replica_offset", uintQS},
	{"merge_tree_min_rows_for_concurrent_read", uintQS},
	{"merge_tree_min_bytes_for_concurrent_read", uintQS},
	{"merge_tree_min_rows_for_seek", uintQS},
	{"merge_tree_min_bytes_for_seek", uintQS},
	{"merge_tree_coarse_index_granularity", uintQS},
	{"merge_tree_max_rows_to_use_cache", uintQS},
	{"merge_tree_max_bytes_to_use_cache", uintQS},
	{"mysql_max_rows_to_insert", uintQS},
	{"optimize_min_equality_disjunction_chain_length", uintQS},
	{"min_bytes_to_use_direct_io", uintQS},
	{"mark_cache_min_lifetime", uintQS},
	{"priority", uintQS},
	{"log_queries_cut_to_length", uintQS},
	{"max_concurrent_queries_for_user", uintQS},
	{"insert_quorum", uintQS},
	{"select_sequential_consistency", uintQS},
	{"table_function_remote_max_addresses", uintQS},
	{"read_backoff_max_throughput", uintQS},
	{"read_backoff_min_events", uintQS},
	{"output_format_pretty_max_rows", uintQS},
	{"output_format_pretty_max_column_pad_width", uintQS},
	{"output_format_parquet_row_group_size", uintQS},
	{"http_headers_progress_interval_ms", uintQS},
	{"input_format_allow_errors_num", uintQS},
	{"preferred_block_size_bytes", uintQS},
	{"max_replica_delay_for_distributed_queries", uintQS},
	{"preferred_max_column_in_block_size_bytes", uintQS},
	{"insert_distributed_timeout", uintQS},
	{"odbc_max_field_size", uintQS},
	{"max_rows_to_read", uintQS},
	{"max_bytes_to_read", uintQS},
	{"max_rows_to_group_by", uintQS},
	{"max_bytes_before_external_group_by", uintQS},
	{"max_rows_to_sort", uintQS},
	{"max_bytes_to_sort", uintQS},
	{"max_bytes_before_external_sort", uintQS},
	{"max_bytes_before_remerge_sort", uintQS},
	{"max_result_rows", uintQS},
	{"max_result_bytes", uintQS},
	{"min_execution_speed", uintQS},
	{"max_execution_speed", uintQS},
	{"min_execution_speed_bytes", uintQS},
	{"max_execution_speed_bytes", uintQS},
	{"max_columns_to_read", uintQS},
	{"max_temporary_columns", uintQS},
	{"max_temporary_non_const_columns", uintQS},
	{"max_subquery_depth", uintQS},
	{"max_pipeline_depth", uintQS},
	{"max_ast_depth", uintQS},
	{"max_ast_elements", uintQS},
	{"max_expanded_ast_elements", uintQS},
	{"readonly", uintQS},
	{"max_rows_in_set", uintQS},
	{"max_bytes_in_set", uintQS},
	{"max_rows_in_join", uintQS},
	{"max_bytes_in_join", uintQS},
	{"max_rows_to_transfer", uintQS},
	{"max_bytes_to_transfer", uintQS},
	{"max_rows_in_distinct", uintQS},
	{"max_bytes_in_distinct", uintQS},
	{"max_memory_usage", uintQS},
	{"max_memory_usage_for_user", uintQS},
	{"max_memory_usage_for_all_queries", uintQS},
	{"max_network_bandwidth", uintQS},
	{"max_network_bytes", uintQS},
	{"max_network_bandwidth_for_user", uintQS},
	{"max_network_bandwidth_for_all_users", uintQS},
	{"low_cardinality_max_dictionary_size", uintQS},
	{"max_fetch_partition_retries_count", uintQS},
	{"http_max_multipart_form_data_size", uintQS},
	{"max_partitions_per_insert_block", uintQS},
	{"max_threads", uintQS},
	{"optimize_skip_unused_shards_nesting", uintQS},
	{"force_optimize_skip_unused_shards", uintQS},
	{"force_optimize_skip_unused_shards_nesting", uintQS},

	{"network_zstd_compression_level", intQS},
	{"http_zlib_compression_level", intQS},
	{"distributed_ddl_task_timeout", intQS},

	{"extremes", boolQS},
	{"use_uncompressed_cache", boolQS},
	{"replace_running_query", boolQS},
	{"distributed_directory_monitor_batch_inserts", boolQS},
	{"optimize_move_to_prewhere", boolQS},
	{"compile", boolQS},
	{"allow_suspicious_low_cardinality_types", boolQS},
	{"compile_expressions", boolQS},
	{"distributed_aggregation_memory_efficient", boolQS},
	{"skip_unavailable_shards", boolQS},
	{"distributed_group_by_no_merge", boolQS},
	{"optimize_skip_unused_shards", boolQS},
	{"merge_tree_uniform_read_distribution", boolQS},
	{"force_index_by_date", boolQS},
	{"force_primary_key", boolQS},
	{"log_queries", boolQS},
	{"insert_deduplicate", boolQS},
	{"enable_http_compression", boolQS},
	{"http_native_compression_disable_checksumming_on_decompress", boolQS},
	{"output_format_write_statistics", boolQS},
	{"add_http_cors_header", boolQS},
	{"input_format_skip_unknown_fields", boolQS},
	{"input_format_with_names_use_header", boolQS},
	{"input_format_import_nested_json", boolQS},
	{"input_format_defaults_for_omitted_fields", boolQS},
	{"input_format_values_interpret_expressions", boolQS},
	{"output_format_json_quote_64bit_integers", boolQS},
	{"output_format_json_quote_denormals", boolQS},
	{"output_format_json_escape_forward_slashes", boolQS},
	{"output_format_pretty_color", boolQS},
	{"use_client_time_zone", boolQS},
	{"send_progress_in_http_headers", boolQS},
	{"fsync_metadata", boolQS},
	{"join_use_nulls", boolQS},
	{"fallback_to_stale_replicas_for_distributed_queries", boolQS},
	{"insert_distributed_sync", boolQS},
	{"insert_allow_materialized_columns", boolQS},
	{"optimize_throw_if_noop", boolQS},
	{"use_index_for_in_with_subqueries", boolQS},
	{"empty_result_for_aggregation_by_empty_set", boolQS},
	{"allow_distributed_ddl", boolQS},
	{"join_any_take_last_row", boolQS},
	{"format_csv_allow_single_quotes", boolQS},
	{"format_csv_allow_double_quotes", boolQS},
	{"log_profile_events", boolQS},
	{"log_query_settings", boolQS},
	{"log_query_threads", boolQS},
	{"enable_optimize_predicate_expression", boolQS},
	{"low_cardinality_use_single_dictionary_for_part", boolQS},
	{"decimal_check_overflow", boolQS},
	{"prefer_localhost_replica", boolQS},
	//{"asterisk_left_columns_only", boolQS},
	{"calculate_text_stack_trace", boolQS},
	{"allow_ddl", boolQS},
	{"parallel_view_processing", boolQS},
	{"enable_debug_queries", boolQS},
	{"enable_unaligned_array_join", boolQS},
	{"low_cardinality_allow_in_native_format", boolQS},
	{"allow_experimental_multiple_joins_emulation", boolQS},
	{"allow_experimental_cross_to_join_conversion", boolQS},
	{"cancel_http_readonly_queries_on_client_close", boolQS},
	{"external_table_functions_use_nulls", boolQS},
	{"allow_experimental_data_skipping_indices", boolQS},
	{"allow_hyperscan", boolQS},
	{"allow_simdjson", boolQS},

	{"connect_timeout", timeQS},
	{"connect_timeout_with_failover_ms", timeQS},
	{"receive_timeout", timeQS},
	{"send_timeout", timeQS},
	{"tcp_keep_alive_timeout", timeQS},
	{"queue_max_wait_ms", timeQS},
	{"distributed_directory_monitor_sleep_time_ms", timeQS},
	{"insert_quorum_timeout", timeQS},
	{"read_backoff_min_latency_ms", timeQS},
	{"read_backoff_min_interval_between_events_ms", timeQS},
	{"stream_flush_interval_ms", timeQS},
	{"stream_poll_timeout_ms", timeQS},
	{"http_connection_timeout", timeQS},
	{"http_send_timeout", timeQS},
	{"http_receive_timeout", timeQS},
	{"max_execution_time", timeQS},
	{"timeout_before_checking_execution_speed", timeQS},
}
