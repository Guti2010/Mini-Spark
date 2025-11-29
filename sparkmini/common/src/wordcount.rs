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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(sub: &str) -> PathBuf {
        let base = std::env::temp_dir()
            .join("wordcount_file_tests")
            .join(sub);
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        base
    }

    /// Caso feliz: archivo con texto normal, mayúsculas, signos, etc.
    #[test]
    fn wordcount_file_counts_words_correctly() {
        let tmp = temp_dir("basic");
        let input_path = tmp.join("input.txt");
        let output_path = tmp.join("out.csv");

        // Creamos un archivo de entrada con varias líneas
        let mut f = fs::File::create(&input_path).unwrap();
        writeln!(f, "Hola hola, mundo!!").unwrap();
        writeln!(f, "mundo   mundo_prueba").unwrap();
        // tokens esperados (normalizados):
        // "hola" x2, "mundo" x2, "mundo_prueba" x1

        wordcount_file(
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort(); // por si el orden cambia

        assert_eq!(lines, vec!["hola,2", "mundo,2", "mundo_prueba,1"]);
    }

    /// Archivo vacío: debe crear un CSV vacío (o sin líneas).
    #[test]
    fn wordcount_file_on_empty_input_creates_empty_output() {
        let tmp = temp_dir("empty");
        let input_path = tmp.join("empty.txt");
        let output_path = tmp.join("out.csv");

        // Archivo vacío
        fs::File::create(&input_path).unwrap();

        wordcount_file(
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.trim().is_empty());
    }

    /// Debe crear directorios intermedios para el output si no existen.
    #[test]
    fn wordcount_file_creates_parent_directory_for_output() {
        let tmp = temp_dir("nested");
        let input_path = tmp.join("input.txt");
        let nested_dir = tmp.join("subdir1").join("subdir2");
        let output_path = nested_dir.join("out.csv");

        let mut f = fs::File::create(&input_path).unwrap();
        writeln!(f, "test test").unwrap();

        // Aún no existe nested_dir
        assert!(!nested_dir.exists());

        wordcount_file(
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .unwrap();

        // Debe haberse creado el directorio y el archivo
        assert!(nested_dir.exists());
        assert!(output_path.exists());

        let content = fs::read_to_string(&output_path).unwrap();
        assert_eq!(content.trim(), "test,2");
    }

    /// Caso de error: archivo de entrada inexistente debe devolver Err.
    #[test]
    fn wordcount_file_returns_error_when_input_missing() {
        let tmp = temp_dir("missing");
        let input_path = tmp.join("no_existe.txt");
        let output_path = tmp.join("out.csv");

        let res = wordcount_file(
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        );

        assert!(res.is_err());
        // y claramente no hay output
        assert!(!output_path.exists());
    }
}

