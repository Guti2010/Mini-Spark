use serde_json::{json, Value};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs::{self, File},
    hash::{Hash, Hasher},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

use crate::dag::Dag;

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

const DEFAULT_MAX_IN_MEM_KEYS: usize = 100_000;

/// Umbral máximo de claves en memoria.
/// Se puede sobreescribir con la env var MAX_IN_MEM_KEYS.
fn max_in_mem_keys() -> usize {
    std::env::var("MAX_IN_MEM_KEYS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_IN_MEM_KEYS)
}

/// Acumulador clave→valor con spill a disco cuando el mapa crece demasiado.
struct SpillingAggregator {
    map: HashMap<String, u64>,
    spill_files: Vec<String>,
    dir: String,
    threshold: usize,
    spill_counter: usize,
}

impl SpillingAggregator {
    fn new(dir: &str, threshold: usize) -> io::Result<Self> {
        if !dir.is_empty() {
            fs::create_dir_all(dir)?;
        }
        Ok(Self {
            map: HashMap::new(),
            spill_files: Vec::new(),
            dir: dir.to_string(),
            threshold,
            spill_counter: 0,
        })
    }

    fn add(&mut self, key: &str, value: u64) -> io::Result<()> {
        *self.map.entry(key.to_string()).or_insert(0) += value;
        if self.map.len() >= self.threshold {
            self.spill_one()?;
        }
        Ok(())
    }

    fn spill_one(&mut self) -> io::Result<()> {
        if self.map.is_empty() || self.dir.is_empty() {
            return Ok(());
        }

        self.spill_counter += 1;
        // ← Hacemos el nombre de spill único por proceso + contador
        let pid = std::process::id();
        let filename = format!("spill-{}-{}.jsonl", pid, self.spill_counter);
        let path = Path::new(&self.dir).join(filename);
        let mut writer = BufWriter::new(File::create(&path)?);

        for (k, v) in self.map.drain() {
            let obj = json!({ "k": k, "v": v });
            serde_json::to_writer(&mut writer, &obj).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("error al escribir spill en {}: {e}", path.display()),
                )
            })?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;
        self.spill_files
            .push(path.to_string_lossy().to_string());
        Ok(())
    }

    fn finalize_to_csv(mut self, output_path: &str) -> io::Result<()> {
        // Combinar mapa en memoria + spills en un acumulador final.
        let mut final_acc: HashMap<String, u64> = HashMap::new();

        for (k, v) in self.map.drain() {
            *final_acc.entry(k).or_insert(0) += v;
        }

        for spill_path in &self.spill_files {
            let file = File::open(spill_path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let v: Value = serde_json::from_str(&line).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error al parsear spill {}: {e}", spill_path),
                    )
                })?;
                let key = v
                    .get("k")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let val = v.get("v").and_then(|x| x.as_u64()).unwrap_or(0);
                *final_acc.entry(key).or_insert(0) += val;
            }
        }

        if let Some(parent) = Path::new(output_path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let out = File::create(output_path)?;
        let mut writer = BufWriter::new(out);

        let mut entries: Vec<(String, u64)> = final_acc.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Igual que antes: líneas "token,count"
        for (key, val) in entries {
            writeln!(writer, "{},{}", key, val)?;
        }

        writer.flush()?;
        Ok(())
    }
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

    // determinista: ordenar por clave
    let mut entries: Vec<(String, u64)> = acc.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    entries
        .into_iter()
        .map(|(k, v)| {
            json!({
                key_field: k,
                value_field: v,
            })
        })
        .collect()
}

/* =========================
   DEMO: WordCount usando operadores (map / flat_map / filter / reduce_by_key)
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

    // Limpia BOM por si viene de Excel/Windows
    let header_line = header_line.trim_start_matches('\u{feff}');

    let headers: Vec<String> = header_line
        .split(',')
        .map(|s| s.trim().trim_start_matches('\u{feff}').to_string())
        .collect();

    for line_res in lines {
        let line = line_res?;
        if line.trim().is_empty() {
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();
        let mut obj = serde_json::Map::new();

        for (idx, h) in headers.iter().enumerate() {
            let mut val = cols.get(idx).unwrap_or(&"").trim();
            // Por si algún valor viene con BOM
            val = val.trim_start_matches('\u{feff}');
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
    //    Usamos un stage_id único por archivo para evitar colisiones entre tareas.
    let file_key = Path::new(input_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("nofile");
    let stage_id = format!("wc_stage1_csv_{}", file_key);

    let partitions = shuffle_to_partitions(
        token_records,
        "token",
        num_partitions,
        tmp_dir,
        &stage_id,
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
    let file_key = Path::new(input_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("nofile");
    let stage_id = format!("wc_stage1_jsonl_{}", file_key);

    let partitions = shuffle_to_partitions(
        token_records,
        "token",
        num_partitions,
        tmp_dir,
        &stage_id,
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
///   key_field,value_field (sin encabezado)
pub fn reduce_partitions_to_file(
    partitions: &[Partition],
    key_field: &str,
    value_field: &str,
    output_path: &str,
) -> io::Result<()> {
    // Si no hay particiones, generamos un archivo vacío y salimos.
    if partitions.is_empty() {
        if let Some(parent) = Path::new(output_path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let _ = File::create(output_path)?;
        return Ok(());
    }

    // Directorio de spill "local" a las particiones, para evitar colisiones entre tareas.
    let first_part_dir = Path::new(&partitions[0].path)
        .parent()
        .unwrap_or_else(|| Path::new("/data/tmp"));

    let spill_dir_path = first_part_dir.join("spill_reduce");
    let spill_dir_str = spill_dir_path.to_string_lossy().to_string();

    let mut agg = SpillingAggregator::new(&spill_dir_str, max_in_mem_keys())?;

    for part in partitions {
        let recs = read_partition(&part.path)?;
        for rec in recs.into_iter() {
            if let Some(obj) = rec.as_object() {
                let key_opt = obj.get(key_field).and_then(|v| v.as_str());
                let val_opt = obj.get(value_field).and_then(|v| v.as_u64());
                if let (Some(k), Some(v)) = (key_opt, val_opt) {
                    agg.add(k, v)?;
                }
            }
        }
    }

    // Escribir el resultado final a CSV (token,count, o la pareja que toque).
    agg.finalize_to_csv(output_path)
}

/* =========================
   Demo local: WordCount con shuffle a particiones
   ========================= */

/// Pipeline de WordCount "estilo Spark" pero en un solo proceso:
///
/// 1. Lee líneas de `input_path`.
/// 2. Genera registros { "token": <palabra>, "count": 1 }.
/// 3. Hace shuffle a N particiones en `tmp_dir/wc_stage1_<archivo>/`.
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
    //    Stage_id único por archivo
    let file_key = Path::new(input_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("nofile");
    let stage_id = format!("wc_stage1_{}", file_key);

    let partitions =
        shuffle_to_partitions(token_records, "token", num_partitions, tmp_dir, &stage_id)?;

    // 4) Reduce: sum(count) por token en todas las particiones
    reduce_partitions_to_file(&partitions, "token", "count", output_path)
}

/* =========================
   Ejecutar DAG de WordCount para un archivo
   ========================= */

/// Ejecuta un DAG de WordCount para **un solo archivo de entrada**.
///
/// Usa sólo el nodo de lectura:
/// - busca un nodo `read_*`
/// - interpreta `op` ("read_csv", "read_jsonl", "read_text", etc.)
/// - usa `partitions` del nodo si viene; si no, usa `num_partitions` por defecto.
///
/// Por ahora asumimos:
///   - CSV/JSONL tienen un campo `"text"` con el contenido.
///   - El resto del pipeline (flat_map/map/reduce_by_key) está fijo para WordCount.
pub fn execute_wordcount_dag_for_file(
    dag: &Dag,
    input_path: &str,
    tmp_dir: &str,
    default_num_partitions: u32,
    output_path: &str,
) -> io::Result<()> {
    // 1) Buscar un nodo de lectura: op que empiece con "read_"
    let read_node = dag
        .nodes
        .iter()
        .find(|n| n.op.starts_with("read_"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "DAG sin nodo read_*"))?;

    // 2) Número de particiones efectivo: el del nodo o el default
    let num_partitions = read_node
        .partitions
        .unwrap_or(default_num_partitions)
        .max(1);

    // 3) Campo de texto para CSV/JSONL (por ahora fijo)
    let text_field = "text";

    // 4) Extensión del archivo (por si necesitamos inferir)
    let ext = Path::new(input_path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    // 5) Determinar formato a partir de op / extensión
    let effective_format = match read_node.op.as_str() {
        "read_csv" => "csv",
        "read_jsonl" => "jsonl",
        "read_json" => "json",
        "read_text" | "read_text_glob" => "text",
        _ => {
            // fallback: inferimos por extensión
            if ext == "csv" {
                "csv"
            } else if ext == "json" || ext == "jsonl" {
                "jsonl"
            } else {
                "text"
            }
        }
    };

    match effective_format {
        "csv" => wordcount_csv_file_shuffled_local(
            input_path,
            text_field,
            tmp_dir,
            num_partitions,
            output_path,
        ),
        "jsonl" | "json" => wordcount_jsonl_file_shuffled_local(
            input_path,
            text_field,
            tmp_dir,
            num_partitions,
            output_path,
        ),
        _ => wordcount_file_shuffled_local(input_path, tmp_dir, num_partitions, output_path),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, fs, io::Write, path::PathBuf};

    fn temp_dir(sub: &str) -> PathBuf {
        let base = env::temp_dir()
            .join("engine_tests")
            .join(sub);
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        base
    }

    /* ============
       OPERADORES
       ============ */

    #[test]
    fn op_map_aplica_funcion_a_cada_registro() {
        let input = vec![json!({"x": 1}), json!({"x": 2})];

        let out = op_map(input, |r| {
            let mut o = r.clone();
            let v = o["x"].as_i64().unwrap();
            o["x"] = json!(v * 10);
            o
        });

        assert_eq!(out, vec![json!({"x": 10}), json!({"x": 20})]);
    }

    #[test]
    fn op_filter_filtra_por_predicado() {
        let input = vec![
            json!({"x": 1}),
            json!({"x": 2}),
            json!({"x": 3}),
        ];

        let out = op_filter(input, |r| r["x"].as_i64().unwrap() % 2 == 1);

        assert_eq!(out, vec![json!({"x": 1}), json!({"x": 3})]);
    }

    #[test]
    fn op_flat_map_expande_registros() {
        let input = vec![
            json!({"nums": [1, 2]}),
            json!({"nums": [3]}),
        ];

        let out = op_flat_map(input, |r| {
            r["nums"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| json!({"n": n}))
                .collect::<Vec<_>>()
        });

        assert_eq!(
            out,
            vec![
                json!({"n": 1}),
                json!({"n": 2}),
                json!({"n": 3}),
            ]
        );
    }

    #[test]
    fn op_reduce_by_key_agrupa_y_suma() {
        let input = vec![
            json!({"token": "a", "count": 1_u64}),
            json!({"token": "b", "count": 1_u64}),
            json!({"token": "a", "count": 2_u64}),
        ];

        let out = op_reduce_by_key(input, "token", "count");

        // reduce_by_key ordena por clave
        assert_eq!(
            out,
            vec![
                json!({"token": "a", "count": 3_u64}),
                json!({"token": "b", "count": 1_u64}),
            ]
        );
    }

    #[test]
    fn op_join_by_key_inner_join_basico() {
        let left = vec![
            json!({"id": "u1", "nombre": "Ana"}),
            json!({"id": "u2", "nombre": "Bob"}),
        ];

        let right = vec![
            json!({"id": "u1", "compras": 10}),
            json!({"id": "u3", "compras": 99}),
        ];

        let out = op_join_by_key(left, right, "id");
        assert_eq!(out.len(), 1);

        let rec = &out[0];
        assert_eq!(rec["id"], json!("u1"));
        assert_eq!(rec["nombre"], json!("Ana"));
        assert_eq!(rec["compras"], json!(10));
    }

    #[test]
    fn merge_records_respeta_campos_izquierda_y_prefija_derecha() {
        let left = json!({"id": "u1", "x": 1, "compartido": "L"});
        let right = json!({"id": "u1", "y": 2, "compartido": "R"});

        let merged = super::merge_records(&left, &right, "id");

        assert_eq!(merged["id"], json!("u1"));
        assert_eq!(merged["x"], json!(1));
        assert_eq!(merged["y"], json!(2));
        // el valor de izquierda se mantiene y el de derecha va con prefijo
        assert_eq!(merged["compartido"], json!("L"));
        assert_eq!(merged["right_compartido"], json!("R"));
    }

    /* =========================
       WORDCOUNT - HELPERS/PIPES
       ========================= */

    #[test]
    fn wc_stage1_make_token_records_normaliza_y_cuenta() {
        let lines = vec!["Hola hola, MUNDO!", "mundo_mundo 123"];

        let recs = super::wc_stage1_make_token_records(lines);

        // tokens: hola x2, mundo x1, mundo_mundo x1, 123 x1
        let mut acc: HashMap<String, u64> = HashMap::new();
        for r in recs {
            let t = r["token"].as_str().unwrap().to_string();
            let c = r["count"].as_u64().unwrap();
            *acc.entry(t).or_insert(0) += c;
        }

        assert_eq!(acc.get("hola"), Some(&2));
        assert_eq!(acc.get("mundo"), Some(&1));
        assert_eq!(acc.get("mundo_mundo"), Some(&1));
        assert_eq!(acc.get("123"), Some(&1));
    }

    #[test]
    fn wc_stage1_from_records_lee_campo_texto() {
        let input = vec![
            json!({"text": "hola mundo"}),
            json!({"text": "hola"}),
        ];

        let recs = super::wc_stage1_from_records(input, "text");
        let mut acc: HashMap<String, u64> = HashMap::new();

        for r in recs {
            let t = r["token"].as_str().unwrap().to_string();
            let c = r["count"].as_u64().unwrap();
            *acc.entry(t).or_insert(0) += c;
        }

        assert_eq!(acc.get("hola"), Some(&2));
        assert_eq!(acc.get("mundo"), Some(&1));
    }

    #[test]
    fn wordcount_from_lines_cuenta_tokens_correctamente() {
        let lines = vec!["hola hola mundo", "mundo a"];

        let out = wordcount_from_lines(lines);

        // tokens: a, hola, mundo
        // a:1, hola:2, mundo:2  (orden alfabético por clave)
        assert_eq!(
            out,
            vec![
                json!({"token": "a",    "count": 1_u64}),
                json!({"token": "hola", "count": 2_u64}),
                json!({"token": "mundo","count": 2_u64}),
            ]
        );
    }

    #[test]
    fn wordcount_from_lines_with_operators_equivale_a_version_simple() {
        let lines = vec!["hola hola mundo", "mundo a"];

        let simple = wordcount_from_lines(lines.clone());
        let via_ops = wordcount_from_lines_with_operators(lines);

        assert_eq!(simple, via_ops);
    }

    /* =========================
       IO: CSV / JSONL / PARTITIONS
       ========================= */

    #[test]
    fn read_csv_to_records_lee_encabezados_y_valores() {
        let tmp = temp_dir("read_csv");
        let csv_path = tmp.join("data.csv");
        let mut f = fs::File::create(&csv_path).unwrap();

        writeln!(f, "nombre,edad").unwrap();
        writeln!(f, "Ana,30").unwrap();
        writeln!(f, "Bob,25").unwrap();

        let recs = read_csv_to_records(csv_path.to_str().unwrap()).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["nombre"], json!("Ana"));
        assert_eq!(recs[0]["edad"], json!("30"));
        assert_eq!(recs[1]["nombre"], json!("Bob"));
    }

    #[test]
    fn read_csv_to_records_soporta_archivo_vacio() {
        let tmp = temp_dir("read_csv_empty");
        let csv_path = tmp.join("data.csv");
        fs::File::create(&csv_path).unwrap(); // sin contenido

        let recs = read_csv_to_records(csv_path.to_str().unwrap()).unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn read_jsonl_to_records_lee_un_objeto_por_linea() {
        let tmp = temp_dir("read_jsonl");
        let jsonl_path = tmp.join("data.jsonl");
        let mut f = fs::File::create(&jsonl_path).unwrap();

        writeln!(f, "{}", r#"{"x":1}"#).unwrap();
        writeln!(f, "{}", r#"{"x":2, "y":"ok"}"#).unwrap();

        let recs = read_jsonl_to_records(jsonl_path.to_str().unwrap()).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["x"], json!(1));
        assert_eq!(recs[1]["y"], json!("ok"));
    }

    #[test]
    fn read_partition_roundtrip_jsonl() {
        let tmp = temp_dir("read_part");
        let p_path = tmp.join("part.jsonl");
        let mut f = fs::File::create(&p_path).unwrap();

        writeln!(f, "{}", r#"{"k":"a","v":1}"#).unwrap();
        writeln!(f, "{}", r#"{"k":"b","v":2}"#).unwrap();

        let recs = read_partition(p_path.to_str().unwrap()).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["k"], json!("a"));
        assert_eq!(recs[1]["v"], json!(2));
    }

    /* =========================
       HASH / SHUFFLE / REDUCE
       ========================= */

    #[test]
    fn hash_key_to_partition_retorna_id_en_rango() {
        let n = 10;
        for key in ["a", "b", "c", "xyz", "otro"] {
            let pid = super::hash_key_to_partition(key, n);
            assert!(pid < n);
        }
    }

    #[test]
    fn shuffle_to_partitions_y_reduce_partitions_to_file_agregan_valores() {
        let tmp = temp_dir("shuffle_reduce");
        let tmp_str = tmp.to_string_lossy().to_string();

        let input = vec![
            json!({"token": "a", "count": 1_u64}),
            json!({"token": "b", "count": 1_u64}),
            json!({"token": "a", "count": 1_u64}),
        ];

        let parts = shuffle_to_partitions(
            input,
            "token",
            2,
            &tmp_str,
            "stage_test",
        )
        .unwrap();

        let out_path = tmp.join("out.csv");
        let out_str = out_path.to_string_lossy().to_string();

        reduce_partitions_to_file(&parts, "token", "count", &out_str).unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort();

        assert_eq!(lines, vec!["a,2", "b,1"]);
    }

    #[test]
    fn reduce_partitions_to_file_con_lista_vacia_crea_archivo_vacio() {
        let tmp = temp_dir("reduce_empty");
        let out_path = tmp.join("out.csv");
        let out_str = out_path.to_string_lossy().to_string();

        reduce_partitions_to_file(&[], "token", "count", &out_str).unwrap();

        assert!(out_path.exists());
        let content = fs::read_to_string(out_path).unwrap();
        assert!(content.trim().is_empty());
    }

    /* =========================
       SPILLING AGGREGATOR
       ========================= */

    #[test]
    fn max_in_mem_keys_respeta_env_var() {
        env::set_var("MAX_IN_MEM_KEYS", "1234");
        assert_eq!(super::max_in_mem_keys(), 1234);
        env::remove_var("MAX_IN_MEM_KEYS");
    }

    #[test]
    fn spilling_aggregator_spillea_y_finaliza_correctamente() {
        let tmp = temp_dir("spill");
        let dir = tmp.join("spill_dir");
        let dir_str = dir.to_string_lossy().to_string();

        // threshold = 2 => al insertar la segunda clave se hace spill
        let mut agg = super::SpillingAggregator::new(&dir_str, 2).unwrap();

        agg.add("a", 1).unwrap(); // mapa: {a:1}
        agg.add("b", 1).unwrap(); // mapa alcanza threshold => spill; se limpia
        agg.add("a", 2).unwrap(); // mapa: {a:2}

        let out_path = tmp.join("final.csv");
        let out_str = out_path.to_string_lossy().to_string();

        agg.finalize_to_csv(&out_str).unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort();

        // de spill: a:1, b:1; de mapa final: a:2 => a:3, b:1
        assert_eq!(lines, vec!["a,3", "b,1"]);
    }

    /* =========================
       PIPELINES “ESTILO SPARK”
       ========================= */

    #[test]
    fn wordcount_file_shuffled_local_end_to_end() {
        let tmp = temp_dir("wc_file");
        let input_path = tmp.join("input.txt");
        let mut f = fs::File::create(&input_path).unwrap();
        writeln!(f, "Hola hola mundo").unwrap();
        writeln!(f, "mundo mundo prueba").unwrap();

        let input_str = input_path.to_string_lossy().to_string();
        let tmp_dir = tmp.join("tmp");
        let tmp_str = tmp_dir.to_string_lossy().to_string();
        let out_path = tmp.join("out.csv");
        let out_str = out_path.to_string_lossy().to_string();

        wordcount_file_shuffled_local(&input_str, &tmp_str, 3, &out_str).unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort();

        // hola x2, mundo x3, prueba x1
        assert_eq!(lines, vec!["hola,2", "mundo,3", "prueba,1"]);
    }

    #[test]
    fn wordcount_csv_file_shuffled_local_usa_columna_text() {
        let tmp = temp_dir("wc_csv");
        let csv_path = tmp.join("data.csv");
        let mut f = fs::File::create(&csv_path).unwrap();
        writeln!(f, "id,text").unwrap();
        writeln!(f, "1,\"hola mundo\"").unwrap();
        writeln!(f, "2,\"hola\"").unwrap();

        let tmp_dir = tmp.join("tmp");
        let tmp_str = tmp_dir.to_string_lossy().to_string();
        let out_path = tmp.join("out.csv");
        let out_str = out_path.to_string_lossy().to_string();

        wordcount_csv_file_shuffled_local(
            csv_path.to_str().unwrap(),
            "text",
            &tmp_str,
            2,
            &out_str,
        )
        .unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort();

        assert_eq!(lines, vec!["hola,2", "mundo,1"]);
    }

    #[test]
    fn wordcount_jsonl_file_shuffled_local_usa_campo_text() {
        let tmp = temp_dir("wc_jsonl");
        let jsonl_path = tmp.join("data.jsonl");
        let mut f = fs::File::create(&jsonl_path).unwrap();
        writeln!(f, "{}", r#"{"text":"hola mundo"}"#).unwrap();
        writeln!(f, "{}", r#"{"text":"hola"}"#).unwrap();

        let tmp_dir = tmp.join("tmp");
        let tmp_str = tmp_dir.to_string_lossy().to_string();
        let out_path = tmp.join("out.csv");
        let out_str = out_path.to_string_lossy().to_string();

        wordcount_jsonl_file_shuffled_local(
            jsonl_path.to_str().unwrap(),
            "text",
            &tmp_str,
            2,
            &out_str,
        )
        .unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let mut lines: Vec<&str> = content.lines().collect();
        lines.sort();

        assert_eq!(lines, vec!["hola,2", "mundo,1"]);
    }

    /* =========================
       JOIN SOBRE PARTICIONES / CSV
       ========================= */

    #[test]
    fn join_partitions_to_jsonl_hace_join_por_id_de_particion() {
        let tmp = temp_dir("join_parts");
        let base = tmp.join("stage");
        fs::create_dir_all(&base).unwrap();

        // Partición izquierda
        let left_path = base.join("part-0-left.jsonl");
        let mut lf = fs::File::create(&left_path).unwrap();
        writeln!(lf, "{}", r#"{"id":"u1","a":1}"#).unwrap();
        writeln!(lf, "{}", r#"{"id":"u2","a":2}"#).unwrap();

        // Partición derecha (mismo id de partición)
        let right_path = base.join("part-0-right.jsonl");
        let mut rf = fs::File::create(&right_path).unwrap();
        writeln!(rf, "{}", r#"{"id":"u1","b":10}"#).unwrap();

        let left_parts = vec![Partition {
            id: 0,
            path: left_path.to_string_lossy().to_string(),
        }];
        let right_parts = vec![Partition {
            id: 0,
            path: right_path.to_string_lossy().to_string(),
        }];

        let out_path = tmp.join("join.jsonl");
        let out_str = out_path.to_string_lossy().to_string();

        join_partitions_to_jsonl(&left_parts, &right_parts, "id", &out_str).unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);

        let rec: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(rec["id"], json!("u1"));
        assert_eq!(rec["a"], json!(1));
        assert_eq!(rec["b"], json!(10));
    }

    #[test]
    fn join_csv_in_memory_hace_inner_join_por_clave() {
        let tmp = temp_dir("join_csv");
        let left_path = tmp.join("left.csv");
        let right_path = tmp.join("right.csv");

        let mut lf = fs::File::create(&left_path).unwrap();
        writeln!(lf, "id,nombre").unwrap();
        writeln!(lf, "u1,Ana").unwrap();
        writeln!(lf, "u2,Bob").unwrap();

        let mut rf = fs::File::create(&right_path).unwrap();
        writeln!(rf, "id,compras").unwrap();
        writeln!(rf, "u1,10").unwrap();
        writeln!(rf, "u3,99").unwrap();

        let out_path = tmp.join("out.jsonl");
        let out_str = out_path.to_string_lossy().to_string();

        join_csv_in_memory(
            left_path.to_str().unwrap(),
            right_path.to_str().unwrap(),
            "id",
            &out_str,
        )
        .unwrap();

        let content = fs::read_to_string(out_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);

        let rec: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(rec["id"], json!("u1"));
        assert_eq!(rec["nombre"], json!("Ana"));
        assert_eq!(rec["compras"], json!("10")); // se leyó como string desde CSV
    }
}



