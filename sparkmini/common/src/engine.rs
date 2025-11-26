use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
    collections::hash_map::DefaultHasher,
};

/// Tipo genérico de registro (fila de datos).
/// Usamos JSON para poder representar texto, CSV, JSONL, etc.
pub type Record = Value;

/// Colección en memoria de registros.
pub type Records = Vec<Record>;

/// Representa una partición física (archivo en disco) de datos.
#[derive(Debug, Clone)]
pub struct Partition {
    pub id: u32,
    pub path: String,
}

/* ---------------- operadores genéricos ---------------- */

/// map: aplica una función a cada registro y devuelve una nueva colección.
pub fn op_map<F>(input: Records, f: F) -> Records
where
    F: Fn(&Record) -> Record,
{
    input.into_iter().map(|rec| f(&rec)).collect()
}

/// filter: deja pasar sólo los registros que cumplan el predicado.
pub fn op_filter<F>(input: Records, f: F) -> Records
where
    F: Fn(&Record) -> bool,
{
    let mut out = Vec::new();
    for rec in input.into_iter() {
        if f(&rec) {
            out.push(rec);
        }
    }
    out
}

/// flat_map: cada registro puede generar cero, uno o muchos registros.
pub fn op_flat_map<F>(input: Records, f: F) -> Records
where
    F: Fn(&Record) -> Vec<Record>,
{
    let mut out = Vec::new();
    for rec in input.iter() {
        let mut v = f(rec);
        out.append(&mut v);
    }
    out
}

/// reduce_by_key:
///   - Agrupa por el campo `key_field` (ej: "token").
///   - Suma el campo numérico `value_field` (ej: "count").
///   - Devuelve registros de la forma `{ key_field: <clave>, value_field: <suma> }`.
pub fn op_reduce_by_key(
    input: Records,
    key_field: &str,
    value_field: &str,
) -> Records {
    let mut acc: HashMap<String, u64> = HashMap::new();

    for rec in input.into_iter() {
        if let Some(obj) = rec.as_object() {
            let key_opt = obj.get(key_field).and_then(|v| v.as_str());
            let val_opt = obj.get(value_field).and_then(|v| v.as_u64());

            if let (Some(key), Some(val)) = (key_opt, val_opt) {
                *acc.entry(key.to_string()).or_insert(0) += val;
            }
        }
    }

    acc.into_iter()
        .map(|(k, v)| {
            json!({
                key_field: k,
                value_field: v,
            })
        })
        .collect()
}

/* ---------------- WordCount en memoria (una sola etapa) ---------------- */

/// Etapa 1 de WordCount:
///   líneas -> registros { "token": <palabra_normalizada>, "count": 1 }
fn wc_stage1_make_token_records<I>(lines: I) -> Records
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut recs: Records = Vec::new();

    for line in lines {
        let line = line.as_ref();
        for raw in line.split_whitespace() {
            let cleaned: String = raw
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '_')
                .collect::<String>()
                .to_lowercase();

            if !cleaned.is_empty() {
                recs.push(json!({
                    "token": cleaned,
                    "count": 1_u64,
                }));
            }
        }
    }

    recs
}

/// Pipeline completo de WordCount en memoria (sin particiones):
/// líneas -> tokens -> reduce_by_key(token, count)
pub fn wordcount_from_lines<I>(lines: I) -> Records
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let recs = wc_stage1_make_token_records(lines);
    op_reduce_by_key(recs, "token", "count")
}

/* ---------------- shuffle a particiones en disco ---------------- */

fn hash_key_to_partition(key: &str, num_partitions: u32) -> u32 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % num_partitions as u64) as u32
}

/// Hace un shuffle de registros a N particiones en disco, usando `key_field`:
///   - Crea carpeta: <base_dir>/<stage_id>/
///   - Crea archivos JSONL: part-0.jsonl, part-1.jsonl, ...
///   - Cada registro se manda según hash(key) % num_partitions.
/// Devuelve los metadatos de las particiones creadas.
pub fn shuffle_to_partitions(
    input: Records,
    key_field: &str,
    num_partitions: u32,
    base_dir: &str,
    stage_id: &str,
) -> io::Result<Vec<Partition>> {
    let stage_dir = Path::new(base_dir).join(stage_id);
    fs::create_dir_all(&stage_dir)?;

    // Abrimos un writer por partición
    let mut writers: Vec<BufWriter<File>> = Vec::new();
    let mut parts: Vec<Partition> = Vec::new();

    for pid in 0..num_partitions {
        let path = stage_dir.join(format!("part-{}.jsonl", pid));
        let file = File::create(&path)?;
        writers.push(BufWriter::new(file));
        parts.push(Partition {
            id: pid,
            path: path.to_string_lossy().to_string(),
        });
    }

    // Escribimos cada registro en la partición que le toca
    for rec in input.into_iter() {
        let key = rec
            .get(key_field)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let pid = hash_key_to_partition(&key, num_partitions) as usize;

        serde_json::to_writer(&mut writers[pid], &rec)?;
        writers[pid].write_all(b"\n")?;
    }

    // Flush de todos los writers
    for w in writers.iter_mut() {
        w.flush()?;
    }

    Ok(parts)
}

/// Lee un archivo de partición (JSONL) y devuelve su contenido como Records.
pub fn read_partition(path: &str) -> io::Result<Records> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: Value = serde_json::from_str(&line)?;
        out.push(rec);
    }

    Ok(out)
}

/// Reduce todas las particiones (que ya tienen {key_field, value_field})
/// y escribe el resultado en un archivo CSV simple:
///   key_field,value_field
pub fn reduce_partitions_to_file(
    partitions: &[Partition],
    key_field: &str,
    value_field: &str,
    output_path: &str,
) -> io::Result<()> {
    let mut acc: HashMap<String, u64> = HashMap::new();

    for part in partitions {
        let recs = read_partition(&part.path)?;
        for rec in recs.into_iter() {
            if let Some(obj) = rec.as_object() {
                let key_opt = obj.get(key_field).and_then(|v| v.as_str());
                let val_opt = obj.get(value_field).and_then(|v| v.as_u64());
                if let (Some(k), Some(v)) = (key_opt, val_opt) {
                    *acc.entry(k.to_string()).or_insert(0) += v;
                }
            }
        }
    }

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let out = File::create(output_path)?;
    let mut writer = BufWriter::new(out);

    let mut entries: Vec<(String, u64)> = acc.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Ejemplo CSV: token,count
    for (key, val) in entries {
        writeln!(writer, "{},{}", key, val)?;
    }

    writer.flush()?;
    Ok(())
}

/* ---------------- demo local: WordCount con shuffle a particiones ---------------- */

/// Pipeline de WordCount "estilo Spark" pero en un solo proceso:
///
/// 1. Lee líneas de `input_path`.
/// 2. Genera registros { "token": <palabra>, "count": 1 }.
/// 3. Hace shuffle a N particiones en `tmp_dir/wc_stage1/`.
/// 4. Reduce todas las particiones y escribe CSV en `output_path`.
pub fn wordcount_file_shuffled_local(
    input_path: &str,
    tmp_dir: &str,
    num_partitions: u32,
    output_path: &str,
) -> io::Result<()> {
    // 1) Leer líneas
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines().map(|l| l.unwrap_or_default());

    // 2) Stage1: líneas -> tokens {token,count=1}
    let token_records = wc_stage1_make_token_records(lines);

    // 3) Shuffle: token -> particiones por hash(token)
    let partitions = shuffle_to_partitions(
        token_records,
        "token",
        num_partitions,
        tmp_dir,
        "wc_stage1",
    )?;

    // 4) Reduce: sum(count) por token en todas las particiones
    reduce_partitions_to_file(&partitions, "token", "count", output_path)
}


// en common::engine

pub fn read_csv_to_records(path: &str) -> io::Result<Records> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();

    // asumiendo primera línea = encabezados
    let mut lines = reader.lines();

    let header_line = match lines.next() {
        Some(l) => l?,
        None => return Ok(out),
    };

    let headers: Vec<String> = header_line
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    for line_res in lines {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();
        let mut obj = serde_json::Map::new();

        for (idx, h) in headers.iter().enumerate() {
            let val = cols.get(idx).unwrap_or(&"").trim();
            obj.insert(h.clone(), json!(val));
        }

        out.push(Value::Object(obj));
    }

    Ok(out)
}

pub fn read_jsonl_to_records(path: &str) -> io::Result<Records> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();

    for line_res in reader.lines() {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: Value = serde_json::from_str(&line)?;
        out.push(rec);
    }

    Ok(out)
}
