use datafusion::execution::context::{SessionConfig, SessionContext};

pub fn new_session_context(batch_size: usize, optimized_p: bool) -> SessionContext {
    let cfg = SessionConfig::default().with_batch_size(batch_size);
    let cfg = if !optimized_p {
        cfg
    } else {
        // These configuration settings originate from
        // https://github.com/tustvold/access-log-bench/blob/b4bdc3895bb16b9e6246332946d085264b8949cd/datafusion/src/main.rs#L27-L32
        //
        // See also:
        // - https://arrow.apache.org/datafusion/user-guide/configs.html
        // - https://github.com/apache/arrow-datafusion/blob/9bee14ebd39dacbb66a9b1f34cd6494bc6a6be3f/datafusion/core/src/config.rs#L61
        cfg.with_collect_statistics(true)
            // use parquet data page level metadata (Page Index) statistics to
            // reduce the number of rows decoded
            .set_bool("datafusion.execution.parquet.enable_page_index", true)
            // filter expressions are be applied during the parquet decoding
            // operation to reduce the number of rows decoded
            .set_bool("datafusion.execution.parquet.pushdown_filters", true)
            // filter expressions evaluated during the parquet decoding opearation
            // will be reordered heuristically to minimize the cost of evaluation
            .set_bool("datafusion.execution.parquet.reorder_filters", true)
    };
    SessionContext::with_config(cfg)
}
