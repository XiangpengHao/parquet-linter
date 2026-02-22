use colored::Colorize;

use crate::benchmark::Measurement;

#[derive(Debug, Clone, Copy)]
pub struct FileResult {
    pub index: usize,
    pub original: Measurement,
    pub output: Measurement,
}

pub fn print(results: &[FileResult]) {
    if results.is_empty() {
        println!("No results.");
        return;
    }

    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "file", "orig_mb", "new_mb", "orig_ms", "new_ms", "orig_cost", "new_cost", "cost_%"
    );

    let mut total_original = 0.0;
    let mut total_output = 0.0;
    let mut total_original_ms = 0.0;
    let mut total_output_ms = 0.0;
    let mut total_original_mb = 0.0;
    let mut total_output_mb = 0.0;

    for result in results {
        total_original += result.original.cost;
        total_output += result.output.cost;
        total_original_ms += result.original.loading_time_ms;
        total_output_ms += result.output.loading_time_ms;
        total_original_mb += result.original.file_size_mb;
        total_output_mb += result.output.file_size_mb;

        let pct = pct_change(result.original.cost, result.output.cost);
        let pct_text = format!("{pct:+.2}%");
        let pct_colored = if pct <= 0.0 {
            pct_text.green()
        } else {
            pct_text.red()
        };

        println!(
            "{:<6} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10}",
            result.index,
            result.original.file_size_mb,
            result.output.file_size_mb,
            result.original.loading_time_ms,
            result.output.loading_time_ms,
            result.original.cost,
            result.output.cost,
            pct_colored
        );
    }

    let total_pct = pct_change(total_original, total_output);
    let total_pct_text = format!("{total_pct:+.2}%");
    let total_pct_colored = if total_pct <= 0.0 {
        total_pct_text.green().bold()
    } else {
        total_pct_text.red().bold()
    };

    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "------", "----------", "----------", "----------", "----------", "----------", "----------", "----------"
    );
    println!(
        "{:<6} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10}",
        "total",
        total_original_mb,
        total_output_mb,
        total_original_ms,
        total_output_ms,
        total_original,
        total_output,
        total_pct_colored
    );

    let size_delta = total_output_mb - total_original_mb;
    let time_delta = total_output_ms - total_original_ms;
    let cost_delta = total_output - total_original;
    let size_delta_pct = pct_change(total_original_mb, total_output_mb);
    let time_delta_pct = pct_change(total_original_ms, total_output_ms);
    let cost_delta_pct = pct_change(total_original, total_output);

    println!();
    println!(
        "Baseline total: size {:.2} MB, time {:.2} ms, cost {:.2}",
        total_original_mb, total_original_ms, total_original
    );
    println!(
        "Rewritten total: size {:.2} MB, time {:.2} ms, cost {:.2}",
        total_output_mb, total_output_ms, total_output
    );
    println!(
        "Diff: size {:+.2} MB ({:+.2}%), time {:+.2} ms ({:+.2}%), cost {:+.2} ({:+.2}%)",
        size_delta,
        size_delta_pct,
        time_delta,
        time_delta_pct,
        cost_delta,
        cost_delta_pct
    );
}

fn pct_change(original: f64, new: f64) -> f64 {
    if original == 0.0 {
        0.0
    } else {
        (new - original) / original * 100.0
    }
}
