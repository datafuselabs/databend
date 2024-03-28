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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::profile::Profile;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::PlanProfile;
use log::info;
use log::warn;
use parking_lot::Condvar;
use parking_lot::Mutex;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::QueryPipelineExecutor;
use crate::pipelines::executor::RunningGraph;

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub type FinishedCallback =
    Box<dyn FnOnce(&Result<Vec<PlanProfile>, ErrorCode>) -> Result<()> + Send + Sync + 'static>;

pub struct QueryWrapper {
    // TODO: will remove it after refactoring queries pipeline executor
    executor: Arc<QueriesPipelineExecutor>,
    graph: Arc<RunningGraph>,
    settings: ExecutorSettings,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_callback: Mutex<Option<FinishedCallback>>,
    #[allow(unused)]
    lock_guards: Vec<LockGuard>,
    finish_condvar_wait: Arc<(Mutex<bool>, Condvar)>,
}

pub enum PipelineExecutor {
    QueryPipelineExecutor(Arc<QueryPipelineExecutor>),
    QueriesPipelineExecutor(QueryWrapper),
}

impl PipelineExecutor {
    pub fn create(mut pipeline: Pipeline, settings: ExecutorSettings) -> Result<Self> {
        if !settings.enable_new_executor {
            Ok(PipelineExecutor::QueryPipelineExecutor(
                QueryPipelineExecutor::create(pipeline, settings)?,
            ))
        } else {
            let on_init_callback = Some(pipeline.take_on_init());
            let on_finished_callback = Some(pipeline.take_on_finished());

            let finish_condvar = Arc::new((Mutex::new(false), Condvar::new()));

            let lock_guards = pipeline.take_lock_guards();

            let graph = RunningGraph::create(
                pipeline,
                1,
                settings.query_id.clone(),
                Some(finish_condvar.clone()),
            )?;

            Ok(PipelineExecutor::QueriesPipelineExecutor(QueryWrapper {
                executor: QueriesPipelineExecutor::create(settings.clone())?,
                graph,
                settings,
                on_init_callback: Mutex::new(on_init_callback),
                on_finished_callback: Mutex::new(on_finished_callback),
                lock_guards,
                finish_condvar_wait: finish_condvar,
            }))
        }
    }

    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Self> {
        if !settings.enable_new_executor {
            Ok(PipelineExecutor::QueryPipelineExecutor(
                QueryPipelineExecutor::from_pipelines(pipelines, settings)?,
            ))
        } else {
            let on_init_callback = {
                let pipelines_callback = pipelines
                    .iter_mut()
                    .map(|x| x.take_on_init())
                    .collect::<Vec<_>>();

                pipelines_callback.into_iter().reduce(|left, right| {
                    Box::new(move || {
                        left()?;
                        right()
                    })
                })
            };

            let on_finished_callback = {
                let pipelines_callback = pipelines
                    .iter_mut()
                    .map(|x| x.take_on_finished())
                    .collect::<Vec<_>>();

                pipelines_callback.into_iter().reduce(|left, right| {
                    Box::new(move |arg| {
                        left(arg)?;
                        right(arg)
                    })
                })
            };

            let finish_condvar = Arc::new((Mutex::new(false), Condvar::new()));

            let lock_guards = pipelines
                .iter_mut()
                .flat_map(|x| x.take_lock_guards())
                .collect::<Vec<_>>();

            let graph = RunningGraph::from_pipelines(
                pipelines,
                1,
                settings.query_id.clone(),
                Some(finish_condvar.clone()),
            )?;

            Ok(PipelineExecutor::QueriesPipelineExecutor(QueryWrapper {
                executor: QueriesPipelineExecutor::create(settings.clone())?,
                graph,
                settings,
                on_init_callback: Mutex::new(on_init_callback),
                on_finished_callback: Mutex::new(on_finished_callback),
                lock_guards,
                finish_condvar_wait: finish_condvar,
            }))
        }
    }

    fn init(on_init_callback: &Mutex<Option<InitCallback>>, query_id: &Arc<String>) -> Result<()> {
        // TODO: the on init callback cannot be killed.
        {
            let instant = Instant::now();
            let mut guard = on_init_callback.lock();
            if let Some(callback) = guard.take() {
                drop(guard);
                if let Err(cause) = Result::flatten(catch_unwind(callback)) {
                    return Err(cause.add_message_back("(while in query pipeline init)"));
                }
            }

            info!(
                "Init pipeline successfully, query_id: {:?}, elapsed: {:?}",
                query_id,
                instant.elapsed()
            );
        }
        Ok(())
    }

    pub fn execute(&self) -> Result<()> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.execute(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                Self::init(
                    &query_wrapper.on_init_callback,
                    &query_wrapper.settings.query_id,
                )?;
                query_wrapper
                    .executor
                    .send_graph(query_wrapper.graph.clone())?;

                let (lock, cvar) = &*query_wrapper.finish_condvar_wait;
                let mut finished = lock.lock();
                if !*finished {
                    cvar.wait(&mut finished);
                }

                let may_error = query_wrapper.graph.get_error();
                match may_error {
                    None => {
                        let guard = query_wrapper.on_finished_callback.lock().take();
                        if let Some(on_finished_callback) = guard {
                            catch_unwind(move || {
                                on_finished_callback(&Ok(self.get_plans_profile()))
                            })??;
                        }
                    }
                    Some(cause) => {
                        let guard = query_wrapper.on_finished_callback.lock().take();
                        if let Some(on_finished_callback) = guard {
                            catch_unwind(move || on_finished_callback(&Err(cause)))??;
                        }
                    }
                }

                Ok(())
            }
        }
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.finish(cause),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => match cause {
                Some(may_error) => {
                    query_wrapper
                        .graph
                        .should_finish(Err(may_error))
                        .expect("executor cannot send error message");
                }
                None => {
                    query_wrapper
                        .graph
                        .should_finish(Ok(()))
                        .expect("executor cannot send error message");
                }
            },
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.is_finished(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                query_wrapper.graph.is_should_finish()
            }
        }
    }

    pub fn format_graph_nodes(&self) -> String {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.format_graph_nodes(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                query_wrapper.graph.format_graph_nodes()
            }
        }
    }

    pub fn get_profiles(&self) -> Vec<Arc<Profile>> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.get_profiles(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                query_wrapper.graph.get_proc_profiles()
            }
        }
    }

    pub fn get_plans_profile(&self) -> Vec<PlanProfile> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.get_plans_profile(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                let mut plans_profile: HashMap<u32, PlanProfile> =
                    HashMap::<u32, PlanProfile>::new();

                for profile in query_wrapper.graph.get_proc_profiles() {
                    if let Some(plan_id) = &profile.plan_id {
                        match plans_profile.entry(*plan_id) {
                            Entry::Occupied(mut v) => {
                                v.get_mut().accumulate(&profile);
                            }
                            Entry::Vacant(v) => {
                                let plan_profile = v.insert(PlanProfile::create(&profile));

                                if let Some(metrics_registry) = &profile.metrics_registry {
                                    match metrics_registry.dump_sample() {
                                        Ok(metrics) => {
                                            plan_profile.add_metrics(
                                                query_wrapper.settings.executor_node_id.clone(),
                                                metrics,
                                            );
                                        }
                                        Err(error) => {
                                            warn!(
                                                "Dump {:?} plan metrics error, cause {:?}",
                                                plan_profile.name, error
                                            );
                                        }
                                    }
                                }
                            }
                        };
                    };
                }

                plans_profile.into_values().collect::<Vec<_>>()
            }
        }
    }
}
