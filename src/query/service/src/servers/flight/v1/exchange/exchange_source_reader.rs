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

use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::EventCause;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use log::info;

use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::RetryableFlightReceiver;

pub struct ExchangeSourceReader {
    finished: AtomicBool,
    output: Arc<OutputPort>,
    output_data: Vec<DataPacket>,
    flight_receiver: RetryableFlightReceiver,
    source: String,
    destination: String,
    fragment: usize,
}

impl ExchangeSourceReader {
    pub fn create(
        output: Arc<OutputPort>,
        flight_receiver: RetryableFlightReceiver,
        source: &str,
        destination: &str,
        fragment: usize,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(ExchangeSourceReader {
            output,
            flight_receiver,
            source: source.to_string(),
            destination: destination.to_string(),
            finished: AtomicBool::new(false),
            output_data: vec![],
            fragment,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeSourceReader {
    fn name(&self) -> String {
        String::from("ExchangeSourceReader")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished.load(Ordering::SeqCst) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            if !self.finished.swap(true, Ordering::SeqCst) {
                self.flight_receiver.close();
            }

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if !self.output_data.is_empty() {
            let packets = std::mem::take(&mut self.output_data);
            let exchange_source_meta = ExchangeDeserializeMeta::create(packets);
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(exchange_source_meta)));
        }

        Ok(Event::Async)
    }

    fn un_reacted(&self, cause: EventCause, id: usize) -> Result<()> {
        if let EventCause::Output(_) = cause {
            if self.output.is_finished() {
                info!("un_reacted output finished, id {}", id);
                if !self.finished.swap(true, Ordering::SeqCst) {
                    self.flight_receiver.close();
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if self.output_data.is_empty() {
            let mut dictionaries = Vec::new();
            while let Some(output_data) = self.flight_receiver.recv().await? {
                if !matches!(&output_data, DataPacket::Dictionary(_)) {
                    dictionaries.push(output_data);
                    self.output_data = dictionaries;
                    return Ok(());
                }

                dictionaries.push(output_data);
            }

            // assert!(dictionaries.is_empty());
        }

        if !self.finished.swap(true, Ordering::SeqCst) {
            self.flight_receiver.close();
        }

        Ok(())
    }

    fn details_status(&self) -> Option<String> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Display {
            source: String,
            destination: String,
            fragment: usize,
            can_push: bool,
        }

        Some(format!("{:?}", Display {
            source: self.source.clone(),
            destination: self.destination.clone(),
            fragment: self.fragment,
            can_push: self.output.can_push()
        }))
    }
}

pub fn create_reader_item(
    flight_receiver: RetryableFlightReceiver,
    source: &str,
    destination: &str,
    fragment: usize,
) -> PipeItem {
    let output = OutputPort::create();
    PipeItem::create(
        ExchangeSourceReader::create(
            output.clone(),
            flight_receiver,
            source,
            destination,
            fragment,
        ),
        vec![],
        vec![output],
    )
}
