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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use databend_common_base::base::tokio;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::variant_transform::contains_variant;
use databend_common_expression::variant_transform::transform_variant;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;
use databend_common_storage::DataOperator;
use opendal::Operator;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub struct TransformUdfScript {
    funcs: Vec<UdfFunctionDesc>,
}

unsafe impl Send for TransformUdfScript {}

impl TransformUdfScript {
    pub fn try_create(
        _func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        log::info!("shamb0, try_create :: {:#?}", funcs);
        Ok(Transformer::create(input, output, Self { funcs }))
    }
}

impl Transform for TransformUdfScript {
    const NAME: &'static str = "UDFScriptTransform";

    fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for func in &self.funcs {
            let num_rows = data_block.num_rows();
            let block_entries = self.prepare_block_entries(&func, &data_block)?;
            let input_batch = self.create_input_batch(block_entries, num_rows)?;

            match func.udf_type.as_script() {
                Some((lang, _, code)) => match lang.as_str() {
                    "javascript" => {
                        self.handle_javascript(&func, &input_batch, &mut data_block, code)?
                    }
                    "python" => self.handle_python(&func, &input_batch, &mut data_block, code)?,
                    "wasm" => self.handle_wasm(&func, &input_batch, &mut data_block, code)?,
                    _ => {
                        return Err(ErrorCode::UDFDataError(format!(
                            "Unsupported script lang: {:#?}",
                            func.udf_type.as_script()
                        )));
                    }
                },
                _ => {
                    return Err(ErrorCode::UDFDataError(format!(
                        "Unsupported script lang: {:#?}",
                        func.udf_type.as_script()
                    )));
                }
            };
        }
        Ok(data_block)
    }
}

impl TransformUdfScript {
    fn prepare_block_entries(
        &self,
        func: &UdfFunctionDesc,
        data_block: &DataBlock,
    ) -> Result<Vec<BlockEntry>> {
        // construct input record_batch
        let block_entries = func
            .arg_indices
            .iter()
            .map(|i| {
                let arg = data_block.get_by_offset(*i).clone();
                if contains_variant(&arg.data_type) {
                    let new_arg = BlockEntry::new(
                        arg.data_type.clone(),
                        transform_variant(&arg.value, true)?,
                    );
                    Ok(new_arg)
                } else {
                    Ok(arg)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(block_entries)
    }

    fn create_input_batch(
        &self,
        block_entries: Vec<BlockEntry>,
        num_rows: usize,
    ) -> Result<RecordBatch> {
        let fields = block_entries
            .iter()
            .enumerate()
            .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
            .collect::<Vec<_>>();
        let data_schema = DataSchema::new(fields);

        let input_batch = DataBlock::new(block_entries, num_rows)
            .to_record_batch_with_dataschema(&data_schema)
            .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

        Ok(input_batch)
    }

    fn update_datablock(
        &self,
        func: &UdfFunctionDesc,
        result_batch: RecordBatch,
        data_block: &mut DataBlock,
    ) -> Result<()> {
        // Convert to DataBlock and add column to data_block
        let schema = DataSchema::try_from(&(*result_batch.schema())).map_err(|err| {
            ErrorCode::UDFDataError(format!("Cannot create schema from record batch: {err}"))
        })?;

        let (result_block, _) =
            DataBlock::from_record_batch(&schema, &result_batch).map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Cannot create data block from record batch: {err}"
                ))
            })?;

        let col = if contains_variant(&func.data_type) {
            let value =
                transform_variant(&result_block.get_by_offset(0).value, false).map_err(|err| {
                    ErrorCode::UDFDataError(format!("Cannot transform variant: {err}"))
                })?;
            BlockEntry {
                data_type: func.data_type.as_ref().clone(),
                value,
            }
        } else {
            result_block.get_by_offset(0).clone()
        };

        data_block.add_column(col);

        Ok(())
    }

    fn handle_javascript(
        &self,
        func: &UdfFunctionDesc,
        input_batch: &RecordBatch,
        data_block: &mut DataBlock,
        code: &str,
    ) -> Result<()> {
        log::info!("Executing JavaScript UDF: {}", func.name);

        let tmp_schema = DataSchema::new(vec![DataField::new(
            "result",
            func.data_type.as_ref().clone(),
        )]);
        let arrow_schema = Schema::from(&tmp_schema);

        let mut js_runtime = arrow_udf_js::Runtime::new()
            .map_err(|err| ErrorCode::UDFDataError(format!("Cannot create js runtime: {err}")))?;

        js_runtime
            .add_function_with_handler(
                &func.name,
                arrow_schema.field(0).data_type().clone(),
                arrow_udf_js::CallMode::ReturnNullOnNullInput,
                code,
                &func.func_name,
            )
            .map_err(|err| ErrorCode::UDFDataError(format!("Cannot add js function: {err}")))?;

        let _ = js_runtime
            .call(&func.name, input_batch)
            .map(|result_batch| self.update_datablock(func, result_batch, data_block))
            .map_err(|err| {
                ErrorCode::from_string(format!(
                    "JS Runtime Call {} execution Error {err}",
                    func.name
                ))
            })?;

        Ok(())
    }

    // TODO: Enable this section after successfully building `arrow_udf_python`.
    fn handle_python(
        &self,
        func: &UdfFunctionDesc,
        input_batch: &RecordBatch,
        data_block: &mut DataBlock,
        code: &str,
    ) -> Result<()> {
        log::info!("Executing Python UDF: {}", func.name);

        let tmp_schema = DataSchema::new(vec![DataField::new(
            "result",
            func.data_type.as_ref().clone(),
        )]);
        let arrow_schema = Schema::from(&tmp_schema);

        let mut py_runtime = arrow_udf_python::Runtime::new().map_err(|err| {
            ErrorCode::UDFDataError(format!("Cannot create python runtime: {err}"))
        })?;

        py_runtime
            .add_function_with_handler(
                &func.name,
                arrow_schema.field(0).data_type().clone(),
                arrow_udf_python::CallMode::ReturnNullOnNullInput,
                code,
                &func.func_name,
            )
            .map_err(|err| ErrorCode::UDFDataError(format!("Cannot add py function: {err}")))?;

        let _ = py_runtime
            .call(&func.name, input_batch)
            .map(|result_batch| self.update_datablock(func, result_batch, data_block))
            .map_err(|err| {
                ErrorCode::from_string(format!(
                    "Py Runtime Call {} execution Error {err}",
                    func.name
                ))
            })?;

        Ok(())
    }

    fn handle_wasm(
        &self,
        func: &UdfFunctionDesc,
        input_batch: &RecordBatch,
        data_block: &mut DataBlock,
        code: &str,
    ) -> Result<()> {
        let code = code.trim();

        log::info!("Executing WASM Module: {} UDF: {}", code, func.name);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // Execute the async block using the created runtime
        let compressed = rt.block_on(async {
            let operator: Operator = DataOperator::instance().operator();
            operator.read(code).await.map_err(|err| {
                ErrorCode::from_string(format!("Failed to read Wasm Module {} {}", code, err))
            })
        })?;

        let mut decoder = DecompressDecoder::new(CompressAlgorithm::Zstd);

        let decompressed = decoder.decompress_all(&compressed).map_err(|err| {
            ErrorCode::from_string(format!("Failed to decompress Wasm Module {} {}", code, err))
        })?;

        log::info!("Wasm module decompression {} Done!!!", code);

        let wasm_runtime = arrow_udf_wasm::Runtime::new(&decompressed)
            .map_err(|err| ErrorCode::UDFDataError(format!("Cannot create wasm runtime: {err}")))?;

        log::info!(
            "WASM runtime initialized with functions: {:?}",
            wasm_runtime.functions().collect::<Vec<_>>()
        );

        let _ = wasm_runtime
            .call(&func.func_name, input_batch)
            .map(|result_batch| self.update_datablock(func, result_batch, data_block))
            .map_err(|err| {
                ErrorCode::from_string(format!(" WASM module call execution Error {} {err}", code))
            })?;

        log::info!("shamb0, transform, wasm module call {} Done!!!", func.name);

        Ok(())
    }
}
