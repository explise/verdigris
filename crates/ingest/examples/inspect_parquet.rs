//! Dump the physical anatomy of a Parquet file: footer, row groups,
//! column chunks, encodings, stats, bloom filters.
//!
//! Usage: cargo run -p verdigris-ingest --example inspect_parquet -- <file.parquet>

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use std::fs::File;

fn human(n: i64) -> String {
    let n = n as f64;
    if n >= 1_048_576.0 {
        format!("{:.1} MB", n / 1_048_576.0)
    } else if n >= 1024.0 {
        format!("{:.1} KB", n / 1024.0)
    } else {
        format!("{} B", n)
    }
}

fn stat_str(s: &Statistics) -> String {
    fn trunc(v: String) -> String {
        if v.len() > 28 {
            format!("{}…", &v[..28])
        } else {
            v
        }
    }
    match s {
        Statistics::Int64(t) => format!(
            "min={:?} max={:?}",
            t.min_opt().copied(),
            t.max_opt().copied()
        ),
        Statistics::ByteArray(t) => format!(
            "min={:?} max={:?}",
            t.min_opt()
                .map(|v| trunc(String::from_utf8_lossy(v.data()).into_owned())),
            t.max_opt()
                .map(|v| trunc(String::from_utf8_lossy(v.data()).into_owned())),
        ),
        other => format!("{other:?}"),
    }
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("usage: inspect_parquet <file>");
    let len = std::fs::metadata(&path)?.len();
    let reader = SerializedFileReader::new(File::open(&path)?)?;
    let meta = reader.metadata();
    let fmeta = meta.file_metadata();

    println!("file: {path}");
    println!("size: {} ({} bytes)", human(len as i64), len);
    println!(
        "rows: {}   row groups: {}   created by: {}",
        fmeta.num_rows(),
        meta.num_row_groups(),
        fmeta.created_by().unwrap_or("?")
    );
    println!("\nschema:");
    for f in fmeta.schema().get_fields() {
        println!("  {:<12} {:?}", f.name(), f.get_physical_type());
    }

    for (i, rg) in meta.row_groups().iter().enumerate() {
        println!(
            "\n── row group {i}: {} rows, {} compressed / {} uncompressed",
            rg.num_rows(),
            human(rg.compressed_size()),
            human(rg.total_byte_size()),
        );
        for col in rg.columns() {
            let name = col.column_path().string();
            let dict = col.dictionary_page_offset().is_some();
            let bloom = col.bloom_filter_offset().is_some();
            println!(
                "  {:<12} {:>9} zstd:{}  dict:{}  bloom:{}  encodings:{:?}",
                name,
                human(col.compressed_size()),
                col.compression() != parquet::basic::Compression::UNCOMPRESSED,
                if dict { "yes" } else { "no " },
                if bloom { "yes" } else { "no " },
                col.encodings().collect::<Vec<_>>(),
            );
            if let Some(s) = col.statistics() {
                println!("               stats: {}", stat_str(s));
            }
        }
    }

    // The physical tail: last 8 bytes = footer length (LE u32) + magic "PAR1".
    let bytes = std::fs::read(&path)?;
    let tail = &bytes[bytes.len() - 8..];
    let footer_len = u32::from_le_bytes(tail[..4].try_into()?);
    println!(
        "\nphysical layout: [PAR1][data pages...][footer: {} thrift metadata][{} = footer len][PAR1]",
        human(footer_len as i64),
        footer_len
    );
    println!(
        "head magic: {:?}   tail magic: {:?}",
        String::from_utf8_lossy(&bytes[..4]),
        String::from_utf8_lossy(&tail[4..])
    );
    Ok(())
}
