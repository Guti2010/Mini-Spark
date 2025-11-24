use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

/// Lee un archivo de texto, cuenta palabras y escribe "palabra,conteo" en output_path.
pub fn wordcount_file(input_path: &str, output_path: &str) -> io::Result<()> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);

    let mut counts: HashMap<String, u64> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        for raw in line.split_whitespace() {
            // limpiar: solo alfanumérico y '_', en minúscula
            let cleaned: String = raw
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_')
                .collect::<String>()
                .to_lowercase();

            if !cleaned.is_empty() {
                *counts.entry(cleaned).or_insert(0) += 1;
            }
        }
    }

    // Crear carpeta de salida si hace falta
    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let out_file = File::create(output_path)?;
    let mut writer = BufWriter::new(out_file);

    let mut entries: Vec<(String, u64)> = counts.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (word, count) in entries {
        writeln!(writer, "{},{}", word, count)?;
    }

    writer.flush()?;
    Ok(())
}
