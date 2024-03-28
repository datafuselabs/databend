// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::mpsc::channel;
use databend_common_base::base::tokio::sync::mpsc::Receiver;
use databend_common_base::base::tokio::sync::mpsc::Sender;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::SyncSenderSink;
use databend_common_pipeline_sources::BlocksSource;
use databend_common_pipeline_transforms::processors::TransformDummy;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::ExecutorWorkerContext;
use databend_query::pipelines::executor::QueryPipelineExecutor;
use databend_query::pipelines::executor::RunningGraph;
use databend_query::pipelines::executor::WorkersCondvar;
use databend_query::pipelines::processors::InputPort;
use databend_query::pipelines::processors::OutputPort;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use parking_lot::Mutex;
use petgraph::stable_graph::NodeIndex;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_simple_pipeline() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    assert_eq!(
        format!("{:?}", create_simple_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"BlocksSource\" ]\
            \n    1 [ label = \"DummyTransform\" ]\
            \n    2 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 1 [ ]\
            \n    1 -> 2 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_parallel_simple_pipeline() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    assert_eq!(
        format!("{:?}", create_parallel_simple_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"BlocksSource\" ]\
            \n    1 [ label = \"BlocksSource\" ]\
            \n    2 [ label = \"DummyTransform\" ]\
            \n    3 [ label = \"DummyTransform\" ]\
            \n    4 [ label = \"SyncSenderSink\" ]\
            \n    5 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 2 [ ]\
            \n    1 -> 3 [ ]\
            \n    2 -> 4 [ ]\
            \n    3 -> 5 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_resize_pipeline() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    assert_eq!(
        format!("{:?}", create_resize_pipeline(ctx)?),
        "digraph {\
            \n    0 [ label = \"BlocksSource\" ]\
            \n    1 [ label = \"Resize\" ]\
            \n    2 [ label = \"DummyTransform\" ]\
            \n    3 [ label = \"DummyTransform\" ]\
            \n    4 [ label = \"Resize\" ]\
            \n    5 [ label = \"DummyTransform\" ]\
            \n    6 [ label = \"Resize\" ]\
            \n    7 [ label = \"SyncSenderSink\" ]\
            \n    8 [ label = \"SyncSenderSink\" ]\
            \n    0 -> 1 [ ]\
            \n    1 -> 2 [ ]\
            \n    1 -> 3 [ ]\
            \n    2 -> 4 [ ]\
            \n    3 -> 4 [ ]\
            \n    4 -> 5 [ ]\
            \n    5 -> 6 [ ]\
            \n    6 -> 7 [ ]\
            \n    6 -> 8 [ ]\
        \n}\n"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_pipeline_init_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    unsafe {
        assert_eq!(
            format!("{:?}", create_simple_pipeline(ctx)?.init_schedule_queue(0)?),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 2, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parallel_simple_pipeline_init_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    unsafe {
        assert_eq!(
            format!(
                "{:?}",
                create_parallel_simple_pipeline(ctx)?.init_schedule_queue(0)?
            ),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 4, name: \"SyncSenderSink\" }, \
                    QueueItem { id: 5, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_resize_pipeline_init_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    unsafe {
        assert_eq!(
            format!("{:?}", create_resize_pipeline(ctx)?.init_schedule_queue(0)?),
            "ScheduleQueue { \
                sync_queue: [\
                    QueueItem { id: 7, name: \"SyncSenderSink\" }, \
                    QueueItem { id: 8, name: \"SyncSenderSink\" }\
                ], \
                async_queue: [] \
            }"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_schedule_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let pipeline = create_simple_pipeline(ctx)?;

    // init queue and result should be sink node
    let init_queue = unsafe { pipeline.clone().init_schedule_queue(0)? };
    unsafe {
        let _ = init_queue.sync_queue.front().unwrap().processor.process();
    }

    // node_indices is input of schedule_queue
    // scheduled_result is result of schedule_queue
    let node_indices = [2, 1, 0, 1, 2];
    let scheduled_result = [1, 0, 1, 2];

    for (i, &index) in node_indices.iter().enumerate() {
        let scheduled = unsafe { pipeline.clone().schedule_queue(NodeIndex::new(index))? };

        assert_eq!(scheduled.sync_queue.len(), if i == 4 { 0 } else { 1 });
        assert_eq!(scheduled.async_queue.len(), 0);

        if i == 4 {
            continue;
        }
        unsafe {
            let _ = scheduled.sync_queue.front().unwrap().processor.process();
            assert_eq!(
                scheduled.sync_queue.front().unwrap().processor.id().index(),
                scheduled_result[i]
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parallel_schedule_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let pipeline = create_parallel_simple_pipeline(ctx)?;

    // init queue and result should be two sink nodes
    let init_queue = unsafe { pipeline.clone().init_schedule_queue(0)? };
    unsafe {
        let _ = init_queue.sync_queue[0].processor.process();
    }
    unsafe {
        let _ = init_queue.sync_queue[1].processor.process();
    }

    // node_indices is input of schedule_queue
    // scheduled_result is result of schedule_queue
    let node_indices = [4, 5, 2, 3, 0, 1, 2, 3, 4, 5];
    let scheduled_result = [2, 3, 0, 1, 2, 3, 4, 5];

    for (i, &index) in node_indices.iter().enumerate() {
        let scheduled = unsafe { pipeline.clone().schedule_queue(NodeIndex::new(index))? };

        assert_eq!(
            scheduled.sync_queue.len(),
            if i == 8 || i == 9 { 0 } else { 1 }
        );
        assert_eq!(scheduled.async_queue.len(), 0);

        if i == 8 || i == 9 {
            continue;
        }
        unsafe {
            let _ = scheduled.sync_queue.front().unwrap().processor.process();
            assert_eq!(
                scheduled.sync_queue.front().unwrap().processor.id().index(),
                scheduled_result[i]
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_resize_schedule_queue() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let pipeline = create_resize_pipeline(ctx)?;

    // init queue and result should be two sink nodes
    let init_queue = unsafe { pipeline.clone().init_schedule_queue(0)? };
    unsafe {
        let _ = init_queue.sync_queue[0].processor.process();
        let _ = init_queue.sync_queue[1].processor.process();
    }

    // node_indices is input of schedule_queue
    // sync_length is length of sync_queue
    // scheduled_result is result of schedule_queue
    let node_indices = [7, 8, 5, 2, 3, 0, 2, 3, 5, 7, 8];
    let sync_length = [1, 0, 2, 1, 0, 2, 0, 1, 2, 0, 0];
    let scheduled_result = [5, 2, 3, 0, 2, 3, 5, 7, 8];
    let mut acc = 0;
    for (i, &index) in node_indices.iter().enumerate() {
        let scheduled = unsafe { pipeline.clone().schedule_queue(NodeIndex::new(index))? };
        assert_eq!(scheduled.sync_queue.len(), sync_length[i]);
        assert_eq!(scheduled.async_queue.len(), 0);

        match sync_length[i] {
            0 => continue,
            1 => unsafe {
                let _ = scheduled.sync_queue.front().unwrap().processor.process();
                assert_eq!(
                    scheduled.sync_queue.front().unwrap().processor.id().index(),
                    scheduled_result[acc]
                );
                acc += 1;
            },
            2 => unsafe {
                let _ = scheduled.sync_queue[0].processor.process();
                let _ = scheduled.sync_queue[1].processor.process();
                assert_eq!(
                    scheduled.sync_queue[0].processor.id().index(),
                    scheduled_result[acc]
                );
                assert_eq!(
                    scheduled.sync_queue[1].processor.id().index(),
                    scheduled_result[acc + 1]
                );
                acc += 2;
            },
            _ => unreachable!(),
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_schedule_queue_twice_without_processing() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let pipeline = create_simple_pipeline(ctx)?;

    let init_queue = unsafe { pipeline.clone().init_schedule_queue(0)? };
    unsafe {
        let _ = init_queue.sync_queue.front().unwrap().processor.process();
    }

    let scheduled = unsafe { pipeline.clone().schedule_queue(NodeIndex::new(2))? };
    assert_eq!(scheduled.sync_queue.len(), 1);

    // schedule a need data node twice, the second time should be ignored and return empty queue
    let scheduled = unsafe { pipeline.clone().schedule_queue(NodeIndex::new(2))? };
    assert_eq!(scheduled.sync_queue.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_schedule_with_one_tasks() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let graph = create_simple_pipeline(ctx.clone())?;

    let executor = create_executor_with_simple_pipeline(ctx, 1).await?;

    let mut context = ExecutorWorkerContext::create(1, WorkersCondvar::create(1));

    let init_queue = unsafe { graph.clone().init_schedule_queue(0)? };
    assert_eq!(init_queue.sync_queue.len(), 1);
    init_queue.schedule(&executor.global_tasks_queue, &mut context, &executor);
    assert!(context.has_task());
    assert_eq!(
        format!("{:?}", context.take_task()),
        "ExecutorTask::Sync { id: 2, name: SyncSenderSink}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_schedule_with_two_tasks() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let graph = create_parallel_simple_pipeline(ctx.clone())?;

    let executor = create_executor_with_simple_pipeline(ctx, 2).await?;

    let mut context = ExecutorWorkerContext::create(1, WorkersCondvar::create(1));

    let init_queue = unsafe { graph.clone().init_schedule_queue(0)? };
    assert_eq!(init_queue.sync_queue.len(), 2);
    init_queue.schedule(&executor.global_tasks_queue, &mut context, &executor);
    assert!(context.has_task());
    assert_eq!(
        format!("{:?}", context.take_task()),
        "ExecutorTask::Sync { id: 4, name: SyncSenderSink}"
    );

    Ok(())
}

fn create_simple_pipeline(ctx: Arc<QueryContext>) -> Result<Arc<RunningGraph>> {
    let (_rx, sink_pipe) = create_sink_pipe(1)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;
    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline, 1, Arc::new("".to_string()), None)
}

fn create_parallel_simple_pipeline(ctx: Arc<QueryContext>) -> Result<Arc<RunningGraph>> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 2)?;

    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline, 1, Arc::new("".to_string()), None)
}

fn create_resize_pipeline(ctx: Arc<QueryContext>) -> Result<Arc<RunningGraph>> {
    let (_rx, sink_pipe) = create_sink_pipe(2)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, 1)?;

    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.try_resize(2)?;
    pipeline.add_pipe(create_transform_pipe(2)?);
    pipeline.try_resize(1)?;
    pipeline.add_pipe(create_transform_pipe(1)?);
    pipeline.try_resize(2)?;
    pipeline.add_pipe(sink_pipe);

    RunningGraph::create(pipeline, 1, Arc::new("".to_string()), None)
}

fn create_source_pipe(
    ctx: Arc<QueryContext>,
    size: usize,
) -> Result<(Vec<Sender<Result<DataBlock>>>, Pipe)> {
    let mut txs = Vec::with_capacity(size);
    let mut items = Vec::with_capacity(size);

    for _index in 0..size {
        let output = OutputPort::create();
        let (tx, _rx) = channel(1);
        txs.push(tx);
        items.push(PipeItem::create(
            BlocksSource::create(
                ctx.clone(),
                output.clone(),
                Arc::new(Mutex::new(VecDeque::new())),
            )?,
            vec![],
            vec![output],
        ));
    }
    Ok((txs, Pipe::create(0, size, items)))
}

fn create_transform_pipe(size: usize) -> Result<Pipe> {
    let mut items = Vec::with_capacity(size);

    for _index in 0..size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    Ok(Pipe::create(size, size, items))
}

fn create_sink_pipe(size: usize) -> Result<(Vec<Receiver<Result<DataBlock>>>, Pipe)> {
    let mut rxs = Vec::with_capacity(size);
    let mut items = Vec::with_capacity(size);
    for _index in 0..size {
        let input = InputPort::create();
        let (tx, rx) = channel(1);
        rxs.push(rx);
        items.push(PipeItem::create(
            ProcessorPtr::create(SyncSenderSink::create(tx, input.clone())),
            vec![input],
            vec![],
        ));
    }

    Ok((rxs, Pipe::create(size, 0, items)))
}

async fn create_executor_with_simple_pipeline(
    ctx: Arc<QueryContext>,
    size: usize,
) -> Result<Arc<QueryPipelineExecutor>> {
    let (_rx, sink_pipe) = create_sink_pipe(size)?;
    let (_tx, source_pipe) = create_source_pipe(ctx, size)?;
    let mut pipeline = Pipeline::create();
    pipeline.add_pipe(source_pipe);
    pipeline.add_pipe(create_transform_pipe(size)?);
    pipeline.add_pipe(sink_pipe);
    pipeline.set_max_threads(size);
    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_new_executor: false,
        max_threads: 8,
        executor_node_id: "".to_string(),
    };
    QueryPipelineExecutor::create(pipeline, settings)
}
