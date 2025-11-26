use serde_json::{json, Value};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
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

/* =========================
   Operadores genéricos
   ========================= */

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
pub fn op_reduce_by_key(input: Records, key_field: &str, value_field: &str) -> Records {
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

/* =========================
   DEMO: WordCount usando los operadores (map / flat_map / filter / reduce_by_key)
   ========================= */

/// map: línea de texto -> Record { "text": <línea> }
fn wc_map_line_to_record(line: &str) -> Record {
    json!({ "text": line })
}

/// flat_map: Record {"text": "..."} -> varios Records {"token": <palabra>, "count": 1}
fn wc_flat_map_tokenize(rec: &Record) -> Vec<Record> {
    let mut out = Vec::new();

    let text = rec
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    for raw in text.split_whitespace() {
        let cleaned: String = raw
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .to_lowercase();

        if !cleaned.is_empty() {
            out.push(json!({
                "token": cleaned,
                "count": 1_u64,
            }));
        }
    }

    out
}

/// filter: dejar sólo Records con "token" no vacío.
fn wc_filter_nonempty(rec: &Record) -> bool {
    rec.get("token")
        .and_then(|v| v.as_str())
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false)
}

/// Pipeline completo de WordCount en memoria,
/// usando explícitamente map -> flat_map -> filter -> reduce_by_key.
/// Esto es tu "mini-Spark" en pequeño.
pub fn wordcount_from_lines_with_operators<I>(lines: I) -> Records
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    // map: línea -> { "text": línea }
    let recs0: Records = lines
        .into_iter()
        .map(|l| wc_map_line_to_record(l.as_ref()))
        .collect();

    // flat_map: {text} -> {token,count=1}*
    let recs1 = op_flat_map(recs0, wc_flat_map_tokenize);

    // filter: tokens no vacíos
    let recs2 = op_filter(recs1, wc_filter_nonempty);

    // reduce_by_key(token, count)
    op_reduce_by_key(recs2, "token", "count")
}

/* =========================
   JOIN en memoria
   ========================= */

/// Fusiona dos registros JSON en uno solo.
/// - el campo `key_field` se mantiene una sola vez
/// - si un campo existe en ambos lados, se respeta el del lado izquierdo
///   y el del derecho se guarda con prefijo `right_`.
fn merge_records(left: &Record, right: &Record, key_field: &str) -> Record {
    let mut obj = serde_json::Map::new();

    if let Some(lobj) = left.as_object() {
        for (k, v) in lobj {
            obj.insert(k.clone(), v.clone());
        }
    }

    if let Some(robj) = right.as_object() {
        for (k, v) in robj {
            if k == key_field {
                // ya existe desde el lado izquierdo; lo dejamos tal cual
                continue;
            }
            if obj.contains_key(k) {
                let new_key = format!("right_{}", k);
                obj.insert(new_key, v.clone());
            } else {
                obj.insert(k.clone(), v.clone());
            }
        }
    }

    Value::Object(obj)
}

/// Inner join en memoria entre dos colecciones por el campo `key_field`.
/// Si hay N registros a la izquierda y M a la derecha con la misma clave,
/// se generan N*M registros combinados.
pub fn op_join_by_key(left: Records, right: Records, key_field: &str) -> Records {
    // indexamos el lado derecho por clave
    let mut index: HashMap<String, Vec<Record>> = HashMap::new();

    for rec in right.into_iter() {
        if let Some(obj) = rec.as_object() {
            if let Some(k) = obj.get(key_field).and_then(|v| v.as_str()) {
                index.entry(k.to_string()).or_default().push(rec);
            }
        }
    }

    let mut out = Vec::new();

    for lrec in left.into_iter() {
        let key_opt = lrec
            .get(key_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let Some(key) = key_opt else {
            continue;
        };

        if let Some(r_matches) = index.get(&key) {
            for rrec in r_matches {
                out.push(merge_records(&lrec, rrec, key_field));
            }
        }
    }

    out
}

/* =========================
   Lectura de archivos a Records
   ========================= */

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

/* =========================
   WordCount en memoria (versión simple)
   ========================= */

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

/// Pipeline completo de WordCount en memoria (sin particiones),
/// usando la etapa 1 + reduce_by_key.
pub fn wordcount_from_lines<I>(lines: I) -> Records
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let recs = wc_stage1_make_token_records(lines);
    op_reduce_by_key(recs, "token", "count")
}

/// Etapa 1 de WordCount desde registros:
///   records con campo `text_field` -> registros { "token": <palabra>, "count": 1 }
fn wc_stage1_from_records(input: Records, text_field: &str) -> Records {
    let mut recs: Records = Vec::new();

    for rec in input.into_iter() {
        if let Some(obj) = rec.as_object() {
            if let Some(text) = obj.get(text_field).and_then(|v| v.as_str()) {
                for raw in text.split_whitespace() {
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
        }
    }

    recs
}

/// WordCount para un archivo CSV.
/// Se asume que el CSV tiene una columna `text_field` con el texto a tokenizar.
pub fn wordcount_csv_file_shuffled_local(
    input_path: &str,
    text_field: &str,
    tmp_dir: &str,
    num_partitions: u32,
    output_path: &str,
) -> io::Result<()> {
    // 1) Leer registros desde CSV
    let recs = read_csv_to_records(input_path)?;

    // 2) Stage1: records -> tokens {token,count=1}
    let token_records = wc_stage1_from_records(recs, text_field);

    // 3) Shuffle: token -> particiones por hash(token)
    let partitions = shuffle_to_partitions(
        token_records,
        "token",
        num_partitions,
        tmp_dir,
        "wc_stage1_csv",
    )?;

    // 4) Reduce: sum(count) por token en todas las particiones
    reduce_partitions_to_file(&partitions, "token", "count", output_path)
}

/// WordCount para un archivo JSONL.
/// Se asume que cada línea es un objeto JSON con un campo `text_field` con el texto.
pub fn wordcount_jsonl_file_shuffled_local(
    input_path: &str,
    text_field: &str,
    tmp_dir: &str,
    num_partitions: u32,
    output_path: &str,
) -> io::Result<()> {
    // 1) Leer registros desde JSONL
    let recs = read_jsonl_to_records(input_path)?;

    // 2) Stage1: records -> tokens {token,count=1}
    let token_records = wc_stage1_from_records(recs, text_field);

    // 3) Shuffle: token -> particiones por hash(token)
    let partitions = shuffle_to_partitions(
        token_records,
        "token",
        num_partitions,
        tmp_dir,
        "wc_stage1_jsonl",
    )?;

    // 4) Reduce: sum(count) por token en todas las particiones
    reduce_partitions_to_file(&partitions, "token", "count", output_path)
}

/* =========================
   Shuffle a particiones en disco
   ========================= */

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

/* =========================
   Demo local: WordCount con shuffle a particiones
   ========================= */

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
    let partitions =
        shuffle_to_partitions(token_records, "token", num_partitions, tmp_dir, "wc_stage1")?;

    // 4) Reduce: sum(count) por token en todas las particiones
    reduce_partitions_to_file(&partitions, "token", "count", output_path)
}

/* =========================
   JOIN sobre particiones en disco
   ========================= */

/// Inner join entre dos conjuntos de particiones ya "shuffeadas" por la misma clave.
/// - `left_parts` y `right_parts` deben venir de `shuffle_to_partitions`
///   usando el mismo `key_field` y el mismo `num_partitions`.
/// - Escribe el resultado en un archivo JSONL en `output_path`.
pub fn join_partitions_to_jsonl(
    left_parts: &[Partition],
    right_parts: &[Partition],
    key_field: &str,
    output_path: &str,
) -> io::Result<()> {
    // indexamos las particiones de la derecha por id
    let mut right_by_id: HashMap<u32, &Partition> = HashMap::new();
    for p in right_parts {
        right_by_id.insert(p.id, p);
    }

    // Crear carpeta de salida si hace falta
    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let out = File::create(output_path)?;
    let mut writer = BufWriter::new(out);

    // Para cada partición izquierda, buscamos la correspondiente derecha
    for lpart in left_parts {
        if let Some(rpart) = right_by_id.get(&lpart.id) {
            let lrecs = read_partition(&lpart.path)?;
            let rrecs = read_partition(&rpart.path)?;

            // join en memoria para esta partición
            let joined = op_join_by_key(lrecs, rrecs, key_field);

            // escribimos los registros como JSONL
            for rec in joined {
                serde_json::to_writer(&mut writer, &rec)?;
                writer.write_all(b"\n")?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

/* =========================
   DEMO: join in-memory sobre dos CSV por clave
   ========================= */

/// Join sencillo entre dos CSV por clave, usando op_join_by_key.
/// - Lee ambos CSV a Records.
/// - Hace inner join por `key_field`.
/// - Escribe la salida como JSONL en `output_path`.
pub fn join_csv_in_memory(
    left_path: &str,
    right_path: &str,
    key_field: &str,
    output_path: &str,
) -> io::Result<()> {
    let left = read_csv_to_records(left_path)?;
    let right = read_csv_to_records(right_path)?;

    let joined = op_join_by_key(left, right, key_field);

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let out = File::create(output_path)?;
    let mut writer = BufWriter::new(out);

    for rec in joined {
        serde_json::to_writer(&mut writer, &rec)?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    Ok(())
}
