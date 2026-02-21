use anyhow::Result;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::path::Path;

pub fn read_metadata(path: &Path) -> Result<ParquetMetaData> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader.metadata().clone())
}

pub fn print_info(path: &Path) -> Result<()> {
    let metadata = read_metadata(path)?;
    let file_meta = metadata.file_metadata();

    println!("File: {}", path.display());
    println!("Version: {}", file_meta.version());
    println!("Created by: {}", file_meta.created_by().unwrap_or("unknown"));
    println!("Num rows: {}", file_meta.num_rows());
    println!("Num row groups: {}", metadata.num_row_groups());
    println!("Schema:");
    print_schema(file_meta.schema_descr());
    println!();

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        println!("Row group {rg_idx}:");
        println!("  Num rows: {}", rg.num_rows());
        println!("  Total byte size: {}", rg.total_byte_size());
        println!("  Num columns: {}", rg.num_columns());
        if let Some(sorting) = rg.sorting_columns() {
            println!("  Sorting columns: {sorting:?}");
        }
        for (col_idx, col) in rg.columns().iter().enumerate() {
            println!("  Column {col_idx} ({}):", col.column_path());
            println!("    Physical type: {:?}", col.column_descr().physical_type());
            println!("    Encodings: {:?}", col.encodings().collect::<Vec<_>>());
            println!("    Compression: {:?}", col.compression());
            println!("    Compressed size: {}", col.compressed_size());
            println!("    Uncompressed size: {}", col.uncompressed_size());
            println!("    Num values: {}", col.num_values());
            println!(
                "    Dictionary page offset: {:?}",
                col.dictionary_page_offset()
            );
            println!("    Bloom filter offset: {:?}", col.bloom_filter_offset());
            println!(
                "    Column index offset: {:?}",
                col.column_index_offset()
            );
            if let Some(stats) = col.statistics() {
                println!("    Has statistics: true");
                if let Some(dc) = stats.distinct_count_opt() {
                    println!("    Distinct count: {dc}");
                }
            }
        }
    }
    Ok(())
}

fn print_schema(schema: &parquet::schema::types::SchemaDescriptor) {
    for i in 0..schema.num_columns() {
        let col = schema.column(i);
        println!(
            "  {}: {:?} (logical: {:?}, rep: {}, def: {})",
            col.path(),
            col.physical_type(),
            col.logical_type_ref(),
            col.max_rep_level(),
            col.max_def_level(),
        );
    }
}
