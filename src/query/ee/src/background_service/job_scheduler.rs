// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::error::TryRecvError;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::sync::Mutex;
use common_base::runtime::Runtime;
use common_base::GLOBAL_TASK;
use common_exception::Result;
use common_meta_app::background::BackgroundJobInfo;
use common_meta_app::background::BackgroundJobState;
use common_meta_app::background::BackgroundJobType;
use dashmap::DashMap;
use log::as_debug;
use log::info;

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;

pub struct JobScheduler {
    one_shot_jobs: DashMap<String, BoxedJob>,
    scheduled_jobs: DashMap<String, BoxedJob>,
    pub job_tick_interval: Duration,
    pub finish_tx: Arc<Mutex<Sender<u64>>>,
    pub finish_rx: Arc<Mutex<Receiver<u64>>>,
    pub suspend_tx: Arc<Mutex<Sender<()>>>,
    pub suspend_rx: Arc<Mutex<Receiver<()>>>,
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new() -> Self {
        let (finish_tx, finish_rx) = tokio::sync::mpsc::channel(100);
        let (suspend_tx, suspend_rx) = tokio::sync::mpsc::channel(100);
        Self {
            one_shot_jobs: DashMap::new(),
            scheduled_jobs: DashMap::new(),
            job_tick_interval: Duration::from_secs(5),
            finish_tx: Arc::new(Mutex::new(finish_tx)),
            finish_rx: Arc::new(Mutex::new(finish_rx)),

            suspend_tx: Arc::new(Mutex::new(suspend_tx)),
            suspend_rx: Arc::new(Mutex::new(suspend_rx)),
        }
    }

    pub fn get_scheduled_job(&self, job_name: &str) -> Option<BoxedJob> {
        self.scheduled_jobs
            .get(job_name)
            .map(|job| job.value().box_clone())
    }

    pub async fn add_job(&mut self, job: impl Job + Send + Sync + Clone + 'static) -> Result<()> {
        let info = &job.get_info().await?;
        if info.job_params.is_none() {
            return Ok(());
        }
        match info.job_params.as_ref().unwrap().job_type {
            BackgroundJobType::ONESHOT => {
                self.one_shot_jobs
                    .insert(job.get_name().name, Box::new(job) as BoxedJob);
            }
            BackgroundJobType::CRON | BackgroundJobType::INTERVAL => {
                self.scheduled_jobs
                    .insert(job.get_name().name, Box::new(job) as BoxedJob);
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        let one_shot_jobs = &self.one_shot_jobs;
        if !one_shot_jobs.is_empty() {
            info!(background = true; "start one_shot jobs");
            Self::check_and_run_jobs(one_shot_jobs).await;
            let mut finished_one_shot_jobs = vec![];
            while let Some(i) = self.finish_rx.clone().lock().await.recv().await {
                finished_one_shot_jobs.push(i);
                if finished_one_shot_jobs.len() == one_shot_jobs.len() {
                    break;
                }
            }
        }
        info!(background = true; "start scheduled jobs");
        self.start_scheduled_jobs(self.job_tick_interval).await?;

        Ok(())
    }

    pub async fn start_scheduled_jobs(&self, tick_duration: std::time::Duration) -> Result<()> {
        let scheduled_jobs = &self.scheduled_jobs;
        if scheduled_jobs.is_empty() {
            return Ok(());
        }
        let mut job_interval = tokio::time::interval(tick_duration);
        loop {
            match self.suspend_rx.lock().await.try_recv() {
                Ok(_) => {
                    info!(background = true; "suspend scheduled jobs");
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    info!(
                        background = true;
                        "suspend channel closed, suspend scheduled jobs"
                    );
                    break;
                }
                _ => {}
            }
            job_interval.tick().await;
            Self::check_and_run_jobs(scheduled_jobs).await;
        }
        Ok(())
    }
    async fn check_and_run_jobs(jobs: &DashMap<String, BoxedJob>) {
        let job_futures = jobs
            .iter()
            .map(|job| {
                let j = job.value().box_clone();
                Self::check_and_run_job(j, false)
            })
            .collect::<Vec<_>>();
        for job in job_futures {
            job.await.unwrap();
        }
    }
    // Checks and runs a single [Job](crate::Job)
    pub async fn check_and_run_job(mut job: BoxedJob, force_execute: bool) -> Result<()> {
        let info = &job.get_info().await?;
        if !Self::should_run_job(info, Utc::now(), force_execute) {
            return Ok(());
        }

        // update job status only if it is not forced to run
        if !force_execute {
            let params = info.job_params.as_ref().unwrap();
            let mut status = info.job_status.clone().unwrap();
            status.next_task_scheduled_time = params.get_next_running_time(Utc::now());
            job.update_job_status(status.clone()).await?;
            info!(background = true, next_scheduled_time = as_debug!(&status.next_task_scheduled_time); "Running job");
        } else {
            info!(background = true; "Running execute job");
        }

        Runtime::spawn_current_runtime(GLOBAL_TASK, async move { job.run().await });
        Ok(())
    }

    // returns true if the job should be run
    pub fn should_run_job(
        job_info: &BackgroundJobInfo,
        time: DateTime<Utc>,
        force_execute: bool,
    ) -> bool {
        if job_info.job_status.clone().is_none() || job_info.job_params.clone().is_none() {
            return false;
        }

        if force_execute {
            return true;
        }

        let job_params = &job_info.job_params.clone().unwrap();
        let job_status = &job_info.job_status.clone().unwrap();
        if job_status.job_state == BackgroundJobState::FAILED
            || job_status.job_state == BackgroundJobState::SUSPENDED
        {
            return false;
        }
        match job_params.job_type {
            BackgroundJobType::INTERVAL | BackgroundJobType::CRON => {
                match job_status.next_task_scheduled_time {
                    None => true,
                    Some(expected) => expected < time,
                }
            }

            BackgroundJobType::ONESHOT => true,
        }
    }
}
