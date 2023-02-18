#include <iostream>
#include "duckdb/function/pragma/pragma_functions.hpp"

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <cctype>

namespace duckdb {

static void PragmaEnableProfilingStatement(ClientContext &context, const FunctionParameters &parameters) {
	context.profiler->automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
	context.profiler->Enable();
}

static void PragmaSetProfilingModeStatement(ClientContext &context, const FunctionParameters &parameters) {
	// this is either profiling_mode = standard, or profiling_mode = detailed
	string mode = StringUtil::Lower(parameters.values[0].ToString());
	if (mode == "standard") {
		context.profiler->Enable();
	} else if (mode == "detailed") {
		context.profiler->DetailedEnable();
	} else {
		throw ParserException("Unrecognized print format %s, supported formats: [standard, detailed]", mode);
	}
}

static void PragmaSetProfilerHistorySize(ClientContext &context, const FunctionParameters &parameters) {
	auto size = parameters.values[0].GetValue<int64_t>();
	if (size <= 0) {
		throw ParserException("Size should be larger than 0");
	}
	context.query_profiler_history->SetProfilerHistorySize(size);
}

static void PragmaEnableProfilingAssignment(ClientContext &context, const FunctionParameters &parameters) {
	// this is either enable_profiling = json, or enable_profiling = query_tree
	string assignment = parameters.values[0].ToString();
	if (assignment == "json") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::JSON;
	} else if (assignment == "query_tree") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else if (assignment == "query_tree_optimizer") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;
	} else {
		throw ParserException(
		    "Unrecognized print format %s, supported formats: [json, query_tree, query_tree_optimizer]", assignment);
	}
	context.profiler->Enable();
}

void RegisterEnableProfiling(BuiltinFunctions &set) {
	vector<PragmaFunction> functions;
	functions.push_back(PragmaFunction::PragmaStatement(string(), PragmaEnableProfilingStatement));
	functions.push_back(
	    PragmaFunction::PragmaAssignment(string(), PragmaEnableProfilingAssignment, LogicalType::VARCHAR));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void PragmaDisableProfiling(ClientContext &context, const FunctionParameters &parameters) {
	context.profiler->Disable();
	context.profiler->automatic_print_format = ProfilerPrintFormat::NONE;
}

static void PragmaProfileOutput(ClientContext &context, const FunctionParameters &parameters) {
	context.profiler->save_location = parameters.values[0].ToString();
}

static void PragmaMemoryLimit(ClientContext &context, const FunctionParameters &parameters) {
	idx_t new_limit = DBConfig::ParseMemoryLimit(parameters.values[0].ToString());
	// set the new limit in the buffer manager
	BufferManager::GetBufferManager(context).SetLimit(new_limit);
}

static void PragmaCollation(ClientContext &context, const FunctionParameters &parameters) {
	auto collation_param = StringUtil::Lower(parameters.values[0].ToString());
	// bind the collation to verify that it exists
	ExpressionBinder::TestCollation(context, collation_param);
	auto &config = DBConfig::GetConfig(context);
	config.collation = collation_param;
}

static void PragmaNullOrder(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = DBConfig::GetConfig(context);
	string new_null_order = StringUtil::Lower(parameters.values[0].ToString());
	if (new_null_order == "nulls first" || new_null_order == "null first" || new_null_order == "first") {
		config.default_null_order = OrderByNullType::NULLS_FIRST;
	} else if (new_null_order == "nulls last" || new_null_order == "null last" || new_null_order == "last") {
		config.default_null_order = OrderByNullType::NULLS_LAST;
	} else {
		throw ParserException("Unrecognized null order '%s', expected either NULLS FIRST or NULLS LAST",
		                      new_null_order);
	}
}

static void PragmaDefaultOrder(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = DBConfig::GetConfig(context);
	string new_order = StringUtil::Lower(parameters.values[0].ToString());
	if (new_order == "ascending" || new_order == "asc") {
		config.default_order_type = OrderType::ASCENDING;
	} else if (new_order == "descending" || new_order == "desc") {
		config.default_order_type = OrderType::DESCENDING;
	} else {
		throw ParserException("Unrecognized order order '%s', expected either ASCENDING or DESCENDING", new_order);
	}
}

static void PragmaSetThreads(ClientContext &context, const FunctionParameters &parameters) {
	auto nr_threads = parameters.values[0].GetValue<int64_t>();
	TaskScheduler::GetScheduler(context).SetThreads(nr_threads);
}

static void PragmaEnableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	context.enable_progress_bar = true;
}

static void PragmaSetProgressBarWaitTime(ClientContext &context, const FunctionParameters &parameters) {
	context.wait_time = parameters.values[0].GetValue<int>();
	context.enable_progress_bar = true;
}

static void PragmaDisableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	context.enable_progress_bar = false;
}

static void PragmaEnablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	context.print_progress_bar = true;
}

static void PragmaDisablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	context.print_progress_bar = false;
}

static void PragmaEnableVerification(ClientContext &context, const FunctionParameters &parameters) {
	context.query_verification_enabled = true;
}

static void PragmaDisableVerification(ClientContext &context, const FunctionParameters &parameters) {
	context.query_verification_enabled = false;
}

static void PragmaEnableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	context.force_parallelism = true;
}

static void PragmaEnableForceIndexJoin(ClientContext &context, const FunctionParameters &parameters) {
	context.force_index_join = true;
}

static void PragmaForceCheckpoint(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).force_checkpoint = true;
}

static void PragmaDisableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	context.force_parallelism = false;
}

static void PragmaEnableForceExternal(ClientContext &context, const FunctionParameters &parameters) {
	context.force_external = true;
}

static void PragmaDisableForceExternal(ClientContext &context, const FunctionParameters &parameters) {
	context.force_external = false;
}

static void PragmaEnableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).object_cache_enable = true;
}

static void PragmaDisableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).object_cache_enable = false;
}

static void PragmaEnableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).checkpoint_on_shutdown = true;
}

static void PragmaDisableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).checkpoint_on_shutdown = false;
}

static void PragmaLogQueryPath(ClientContext &context, const FunctionParameters &parameters) {
	auto str_val = parameters.values[0].ToString();
	if (str_val.empty()) {
		// empty path: clean up query writer
		context.log_query_writer = nullptr;
	} else {
		context.log_query_writer = make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(context), str_val);
	}
}

static void PragmaExplainOutput(ClientContext &context, const FunctionParameters &parameters) {
	string val = StringUtil::Lower(parameters.values[0].ToString());
	if (val == "all") {
		context.explain_output_type = ExplainOutputType::ALL;
	} else if (val == "optimized_only") {
		context.explain_output_type = ExplainOutputType::OPTIMIZED_ONLY;
	} else if (val == "physical_only") {
		context.explain_output_type = ExplainOutputType::PHYSICAL_ONLY;
	} else {
		throw ParserException("Unrecognized output type '%s', expected either ALL, OPTIMIZED_ONLY or PHYSICAL_ONLY",
		                      val);
	}
}

static void PragmaEnableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	context.enable_optimizer = true;
}

static void PragmaDisableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	context.enable_optimizer = false;
}

static void PragmaPerfectHashThreshold(ClientContext &context, const FunctionParameters &parameters) {
	auto bits = parameters.values[0].GetValue<int32_t>();
	;
	if (bits < 0 || bits > 32) {
		throw ParserException("Perfect HT threshold out of range: should be within range 0 - 32");
	}
	context.perfect_ht_threshold = bits;
}

static void PragmaAutoCheckpointThreshold(ClientContext &context, const FunctionParameters &parameters) {
	idx_t new_limit = DBConfig::ParseMemoryLimit(parameters.values[0].ToString());
	DBConfig::GetConfig(context).checkpoint_wal_size = new_limit;
}

static void PragmaDebugCheckpointAbort(ClientContext &context, const FunctionParameters &parameters) {
	auto checkpoint_abort = StringUtil::Lower(parameters.values[0].ToString());
	auto &config = DBConfig::GetConfig(context);
	if (checkpoint_abort == "none") {
		config.checkpoint_abort = CheckpointAbort::NO_ABORT;
	} else if (checkpoint_abort == "before_truncate") {
		config.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE;
	} else if (checkpoint_abort == "before_header") {
		config.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER;
	} else if (checkpoint_abort == "after_free_list_write") {
		config.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE;
	} else {
		throw ParserException(
		    "Unrecognized option for PRAGMA debug_checkpoint_abort, expected none, before_truncate or before_header");
	}
}

static void PragmaSetTempDirectory(ClientContext &context, const FunctionParameters &parameters) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	buffer_manager.SetTemporaryDirectory(parameters.values[0].ToString());
}

static void PragmaForceCompression(ClientContext &context, const FunctionParameters &parameters) {
	auto compression = StringUtil::Lower(parameters.values[0].ToString());
	auto &config = DBConfig::GetConfig(context);
	if (compression == "none") {
		config.force_compression = CompressionType::COMPRESSION_INVALID;
	} else {
		auto compression_type = CompressionTypeFromString(compression);
		if (compression_type == CompressionType::COMPRESSION_INVALID) {
			throw ParserException("Unrecognized option for PRAGMA force_compression, expected none, uncompressed, rle, "
			                      "dictionary, pfor, bitpacking or fsst");
		}
		config.force_compression = compression_type;
	}
}

static void PragmaDebugManyFreeListBlocks(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = DBConfig::GetConfig(context);
	config.debug_many_free_list_blocks = true;
}

static void PragmaSetJoin(ClientContext &context, const FunctionParameters &parameters) {
	string join_type = parameters.values[0].ToString();
	D_ASSERT(join_type == "hash" || join_type == "merge" || join_type == "nl" || join_type == "index" || join_type == "block" || join_type == "clear");
	std::cout << "Setting join type to " << join_type << " - be careful! Failures possible for hash/index join if non equijoin." << std::endl;
	if (join_type == "clear") {
		context.explict_join_type = nullptr;
	} else {
		context.explict_join_type = make_unique<string>(join_type);
	}
}

static void PragmaSetAgg(ClientContext &context, const FunctionParameters &parameters) {
	string agg_type = parameters.values[0].ToString();
	D_ASSERT(agg_type == "perfect" || agg_type == "reg" || agg_type == "clear");
	std::cout << "Setting agg type to " << agg_type << " - be careful! Failures possible if too many buckets (I think)." << std::endl;
	if (agg_type == "clear") {
		context.explicit_agg_type = nullptr;
	} else {
		context.explicit_agg_type = make_unique<string>(agg_type);
	}
}

static void PragmaSetFilter(ClientContext &context, const FunctionParameters &parameters) {
	string filter_type = parameters.values[0].ToString();
	D_ASSERT(filter_type == "filter" || filter_type == "clear");
	std::cout << "Setting filter type to " << filter_type << " - be careful!" << std::endl;
	if (filter_type == "clear") {
		context.explicit_filter_type = nullptr;
	} else {
		context.explicit_filter_type = make_unique<string>(filter_type);
	}
}

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	RegisterEnableProfiling(set);

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("profiling_mode", PragmaSetProfilingModeStatement, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("set_profiler_history_size", PragmaSetProfilerHistorySize,
	                                                 LogicalType::BIGINT));

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", PragmaDisableProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", PragmaDisableProfiling));

	set.AddFunction(PragmaFunction::PragmaAssignment("profile_output", PragmaProfileOutput, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("profiling_output", PragmaProfileOutput, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("memory_limit", PragmaMemoryLimit, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("collation", PragmaCollation, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_collation", PragmaCollation, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("null_order", PragmaNullOrder, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_null_order", PragmaNullOrder, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("order", PragmaDefaultOrder, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_order", PragmaDefaultOrder, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("threads", PragmaSetThreads, LogicalType::BIGINT));
	set.AddFunction(PragmaFunction::PragmaAssignment("worker_threads", PragmaSetThreads, LogicalType::BIGINT));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", PragmaEnableVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", PragmaDisableVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("force_parallelism", PragmaEnableForceParallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_force_parallelism", PragmaDisableForceParallelism));

	set.AddFunction(PragmaFunction::PragmaStatement("force_external", PragmaEnableForceExternal));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_force_external", PragmaDisableForceExternal));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_object_cache", PragmaEnableObjectCache));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_object_cache", PragmaDisableObjectCache));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_optimizer", PragmaEnableOptimizer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_optimizer", PragmaDisableOptimizer));

	set.AddFunction(PragmaFunction::PragmaAssignment("log_query_path", PragmaLogQueryPath, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("explain_output", PragmaExplainOutput, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaStatement("force_index_join", PragmaEnableForceIndexJoin));
	set.AddFunction(PragmaFunction::PragmaStatement("force_checkpoint", PragmaForceCheckpoint));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("set_progress_bar_time", PragmaSetProgressBarWaitTime, LogicalType::INTEGER));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("perfect_ht_threshold", PragmaPerfectHashThreshold, LogicalType::INTEGER));

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("wal_autocheckpoint", PragmaAutoCheckpointThreshold, LogicalType::VARCHAR));
	set.AddFunction(
	    PragmaFunction::PragmaAssignment("checkpoint_threshold", PragmaAutoCheckpointThreshold, LogicalType::VARCHAR));

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("debug_checkpoint_abort", PragmaDebugCheckpointAbort, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("temp_directory", PragmaSetTempDirectory, LogicalType::VARCHAR));

	set.AddFunction(
	    PragmaFunction::PragmaAssignment("force_compression", PragmaForceCompression, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaStatement("debug_many_free_list_blocks", PragmaDebugManyFreeListBlocks));
	set.AddFunction(PragmaFunction::PragmaAssignment("set_join", PragmaSetJoin, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("set_agg", PragmaSetAgg, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("set_filter", PragmaSetFilter, LogicalType::VARCHAR));
}

} // namespace duckdb
