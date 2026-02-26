mod compression_codec;
mod compression_ratio;
mod dictionary_encoding;
mod float_encoding;
mod gpu_page_count;
mod page_size;
mod page_statistics;
mod string_encoding;
mod string_statistics;
mod timestamp_encoding;
mod vector_embedding;

use crate::rule::Rule;

pub fn all_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(compression_ratio::CompressionRatioRule),
        Box::new(page_statistics::PageStatisticsRule),
        Box::new(vector_embedding::VectorEmbeddingRule),
        Box::new(dictionary_encoding::DictionaryEncodingRule),
        Box::new(page_size::PageSizeRule),
        Box::new(float_encoding::FloatEncodingRule),
        Box::new(gpu_page_count::GpuPageCountRule),
        Box::new(string_encoding::StringEncodingRule),
        Box::new(compression_codec::CompressionCodecRule),
        Box::new(timestamp_encoding::TimestampEncodingRule),
        Box::new(string_statistics::StringStatisticsRule),
    ]
}

pub fn get_rules(names: Option<&[String]>) -> Vec<Box<dyn Rule>> {
    let all = all_rules();
    match names {
        None => all,
        Some(names) => all
            .into_iter()
            .filter(|r| names.iter().any(|n| n == r.name()))
            .collect(),
    }
}
