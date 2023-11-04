// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Write;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_ast::ast::Statement;
use common_base::base::tokio;
use common_base::base::tokio::io::AsyncWriteExt;
use common_base::base::ProgressValues;
use common_base::runtime::Runtime;
use common_exception::Result;
use common_expression::block_debug::box_render;
use common_expression::infer_table_schema;
use common_expression::DataSchemaRef;
use common_expression::SendableDataBlockStream;
use common_formats::FileFormatOptionsExt;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::StageFileFormatType;
use common_sql::plans::Plan;
use common_storages_fuse::TableContext;
use futures::StreamExt;
use indicatif::HumanBytes;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use rustyline::highlight::Highlighter;
use tokio::time::Instant;

use super::config::OutputFormat;
use super::config::Settings;
use crate::local::helper::CliHelper;
use crate::sessions::QueryContext;

#[async_trait::async_trait]
pub trait ChunkDisplay {
    async fn display(&mut self) -> Result<()>;
}

pub struct FormatDisplay<'a> {
    ctx: Arc<QueryContext>,
    is_repl: bool,
    settings: &'a Settings,
    stmt: Statement,
    // whether replace '\n' with '\\n',
    // disable in explain/show create stmts or user config setting false
    replace_newline: bool,
    plan: Plan,
    schema: DataSchemaRef,
    stream: SendableDataBlockStream,
    rows: usize,
    start: Instant,
}

impl<'a> FormatDisplay<'a> {
    pub fn new(
        ctx: Arc<QueryContext>,
        is_repl: bool,
        settings: &'a Settings,
        stmt: Statement,
        start: Instant,
        plan: Plan,
        stream: SendableDataBlockStream,
    ) -> Self {
        let replace_newline = !if settings.replace_newline {
            false
        } else {
            replace_newline_in_box_display(&stmt)
        };

        let schema = plan.schema();
        Self {
            settings,
            is_repl,
            stmt,
            plan,
            rows: 0,
            schema,
            stream,
            replace_newline,
            start,
            ctx,
        }
    }
}

impl<'a> FormatDisplay<'a> {
    async fn display_progress(progress_bar: &mut Option<ProgressBar>, pg: &QueryProgress) {
        let pgo = progress_bar.take();
        *progress_bar = Some(display_read_progress(pgo, pg));
    }

    async fn display_table(&mut self) -> Result<()> {
        if self.settings.display_pretty_sql && !self.is_repl {
            let format_sql = self.stmt.to_string();
            let format_sql = CliHelper::new().highlight(&format_sql, format_sql.len());
            println!("\n{}\n", format_sql);
        }

        let mut error = None;

        const TICK_MS: usize = 20;
        const MAX_WAIT_MS: usize = 500;
        const MIN_PERCENT_PROGRESS: usize = 3;

        let total_scan_value = self.ctx.get_total_scan_value();

        async fn get_display_progress(
            is_final: bool,
            current_scan_value: &mut ProgressValues,
            wait_times: &mut usize,
            ctx: &Arc<QueryContext>,
            bar: &mut Option<ProgressBar>,
            total_scan_value: &ProgressValues,
        ) -> Option<QueryProgress> {
            let progress = ctx.get_scan_progress_value();

            if !is_final
                && progress.bytes - current_scan_value.bytes
                    < total_scan_value.bytes * MIN_PERCENT_PROGRESS / 100
                && *wait_times < MAX_WAIT_MS / TICK_MS
            {
                *wait_times += 1;
                return None;
            }

            let write_progress = ctx.get_write_progress_value();

            let pg = QueryProgress {
                total_rows: total_scan_value.rows,
                total_bytes: total_scan_value.bytes,

                read_rows: progress.rows,
                read_bytes: progress.bytes,
                write_rows: write_progress.rows,
                write_bytes: write_progress.bytes,
            };
            FormatDisplay::display_progress(bar, &pg).await;
            Some(pg)
        }

        self.rows = 0;
        let is_finished = Arc::new(AtomicBool::new(false));
        let is_finished_clone = is_finished.clone();

        let ctx = self.ctx.clone();

        let is_repl = self.is_repl;

        let handle = Runtime::spawn_current_runtime(ctx.get_id(), async move {
            if !is_repl {
                return (None, None);
            }
            let mut wait_times = 0;
            let mut current_scan_value = ctx.get_scan_progress_value();

            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(TICK_MS as u64));

            let mut bar = None;
            while !is_finished.load(Ordering::SeqCst) {
                interval.tick().await;
                let _ = get_display_progress(
                    false,
                    &mut current_scan_value,
                    &mut wait_times,
                    &ctx,
                    &mut bar,
                    &total_scan_value,
                )
                .await;
            }
            let stats = get_display_progress(
                true,
                &mut current_scan_value,
                &mut wait_times,
                &ctx,
                &mut bar,
                &total_scan_value,
            )
            .await;

            (stats, bar)
        });

        let mut blocks = Vec::new();
        while let Some(item) = self.stream.next().await {
            match item {
                Ok(datablock) => {
                    self.rows += datablock.num_rows();
                    blocks.push(datablock);
                }
                Err(err) => {
                    error = Some(err);
                    break;
                }
            }
        }

        is_finished_clone.store(true, Ordering::SeqCst);

        let (stats, mut bar) = handle.await.unwrap();
        if let Some(pb) = bar.take() {
            pb.finish_and_clear();
        }

        if !blocks.is_empty() {
            println!(
                "{}",
                box_render(
                    &self.schema,
                    &blocks,
                    self.settings.max_display_rows,
                    self.settings.max_width,
                    self.settings.max_col_width,
                    self.replace_newline,
                )?
            );
        }

        if let Some(err) = error {
            eprintln!("error happens after fetched {} rows: {}", self.rows, err);
        }

        self.display_stats(stats).await;
        Ok(())
    }

    async fn display_stats(&mut self, mut stats: Option<QueryProgress>) {
        if !self.settings.show_stats || !self.is_repl {
            return;
        }

        if let Some(ref mut stats) = stats {
            stats.normalize();

            let (rows, kind) = if !self.plan.has_result_set() {
                (stats.write_rows, "written")
            } else {
                (self.rows, "result")
            };

            let rows_str = if rows > 1 { "rows" } else { "row" };
            eprintln!(
                "{} {} {kind} in {:.3} sec. Processed {} rows, {} ({} rows/s, {}/s)",
                rows,
                rows_str,
                self.start.elapsed().as_secs_f64(),
                humanize_count(stats.total_rows as f64),
                HumanBytes(stats.total_rows as u64),
                humanize_count(stats.total_rows as f64 / self.start.elapsed().as_secs_f64()),
                HumanBytes((stats.total_bytes as f64 / self.start.elapsed().as_secs_f64()) as u64),
            );
            eprintln!();
        }
    }

    async fn display_common_formats(&mut self) -> Result<()> {
        let name = format!("{:?}", self.settings.output_format);
        let mut options_ext =
            FileFormatOptionsExt::create_from_settings(&self.ctx.get_settings(), true)?;

        let table_schema = infer_table_schema(&self.schema)?;
        let stage_type = StageFileFormatType::from_str(&name)?;
        let params = FileFormatParams::default_by_type(stage_type)?;

        let mut output_format = options_ext.get_output_format(table_schema, params)?;

        let mut stdout = tokio::io::stdout();
        let prefix = output_format.serialize_prefix()?;
        stdout.write_all(&prefix).await?;

        while let Some(Ok(block)) = self.stream.next().await {
            let data = output_format.serialize_block(&block)?;
            stdout.write_all(&data).await?;
        }
        let f = output_format.finalize()?;
        stdout.write_all(&f).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<'a> ChunkDisplay for FormatDisplay<'a> {
    async fn display(&mut self) -> Result<()> {
        match self.settings.output_format {
            OutputFormat::Null => {}
            OutputFormat::Table => {
                self.display_table().await?;
            }
            _ => self.display_common_formats().await?,
        }

        Ok(())
    }
}

fn format_read_progress(progress: &QueryProgress, elapsed: f64) -> String {
    format!(
        "Processing {}/{} ({} rows/s), {}/{} ({}/s)",
        humanize_count(progress.read_rows as f64),
        humanize_count(progress.total_rows as f64),
        humanize_count(progress.read_rows as f64 / elapsed),
        HumanBytes(progress.read_bytes as u64),
        HumanBytes(progress.total_bytes as u64),
        HumanBytes((progress.read_bytes as f64 / elapsed) as u64)
    )
}

fn display_read_progress(pb: Option<ProgressBar>, current: &QueryProgress) -> ProgressBar {
    let pb = pb.unwrap_or_else(|| {
        let pbn = ProgressBar::new(current.total_bytes as u64);
        let progress_color = "green";
        let template = "{spinner:.${progress_color}} [{elapsed_precise}] {msg} {wide_bar:.${progress_color}/blue} ({eta})".replace("${progress_color}", progress_color);
        pbn.set_style(
            ProgressStyle::with_template(&template)
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                })
                .progress_chars("█▓▒░ "),
        );
        pbn
    });

    pb.set_position(current.read_bytes as u64);
    pb.set_message(format_read_progress(current, pb.elapsed().as_secs_f64()));
    pb
}

pub fn humanize_count(num: f64) -> String {
    if num == 0.0 {
        return String::from("0");
    }

    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = [
        "",
        " thousand",
        " million",
        " billion",
        " trillion",
        " quadrillion",
    ];

    if num < 1_f64 {
        return format!("{}{:.2}", negative, num);
    }
    let delimiter = 1000_f64;
    let exponent = std::cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{}{}{}", negative, pretty_bytes, unit)
}

fn replace_newline_in_box_display(stmt: &Statement) -> bool {
    !matches!(
        stmt,
        Statement::Explain { .. }
            | Statement::ShowCreateCatalog(_)
            | Statement::ShowCreateDatabase(_)
            | Statement::ShowCreateTable(_)
    )
}

#[derive(Clone, Debug, Default)]
pub struct QueryProgress {
    pub total_rows: usize,
    pub total_bytes: usize,

    pub read_rows: usize,
    pub read_bytes: usize,

    pub write_rows: usize,
    pub write_bytes: usize,
}

impl QueryProgress {
    pub fn normalize(&mut self) {
        if self.total_rows == 0 {
            self.total_rows = self.read_rows;
        }
        if self.total_bytes == 0 {
            self.total_bytes = self.read_bytes;
        }
    }
}
