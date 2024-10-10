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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_pipeline_core::PlanProfile;
use databend_common_storages_system::ProfilesLogQueue;
use http::StatusCode;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;

use crate::clusters::ClusterDiscovery;
use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::servers::flight::v1::actions::GET_PROFILE;
use crate::sessions::SessionManager;

#[poem::handler]
#[async_backtrace::framed]
pub async fn query_profiling_handler(
    Path(query_id): Path<String>,
) -> poem::Result<impl IntoResponse> {
    #[derive(serde::Serialize)]
    struct QueryProfiles {
        query_id: String,
        profiles: Vec<PlanProfile>,
        statistics_desc: Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>,
    }
    match get_profile_from_cache(&query_id) {
        Ok(profiles) => {
            return Ok(Json(QueryProfiles {
                query_id: query_id.clone(),
                profiles,
                statistics_desc: get_statistics_desc(),
            }));
        }
        Err(cause) => {
            if cause.code() != ErrorCode::UNKNOWN_QUERY {
                return Err(poem::Error::from_string(
                    format!("Failed to fetch profile from cache queue. cause: {cause}"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
        }
    }
    let res = match SessionManager::instance().get_query_profiles(&query_id) {
        Ok(profiles) => profiles,
        Err(cause) => match cause.code() == ErrorCode::UNKNOWN_QUERY {
            true => match get_cluster_profile(&query_id).await {
                Ok(profiles) => profiles,
                Err(cause) => {
                    return Err(match cause.code() == ErrorCode::UNKNOWN_QUERY {
                        true => poem::Error::from_string(cause.message(), StatusCode::NOT_FOUND),
                        false => poem::Error::from_string(
                            format!("Failed to fetch cluster node profile. cause: {cause}"),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        ),
                    });
                }
            },
            false => {
                return Err(poem::Error::from_string(
                    format!("Failed to fetch cluster node profile. cause: {cause}"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
        },
    };

    Ok(Json(QueryProfiles {
        query_id: query_id.clone(),
        profiles: res,
        statistics_desc: get_statistics_desc(),
    }))
}

async fn get_cluster_profile(query_id: &str) -> Result<Vec<PlanProfile>, ErrorCode> {
    let config = GlobalConfig::instance();
    let cluster = ClusterDiscovery::instance().discover(&config).await?;

    let mut message = HashMap::with_capacity(cluster.nodes.len());

    for node_info in &cluster.nodes {
        if node_info.id != cluster.local_id {
            message.insert(node_info.id.clone(), query_id.to_owned());
        }
    }
    let flight_params = FlightParams {
        timeout: 60,
        retry_times: 3,
        retry_interval: 3,
    };
    let res = cluster
        .do_action::<_, Option<Vec<PlanProfile>>>(GET_PROFILE, message, flight_params)
        .await?;

    match res.into_values().find(Option::is_some) {
        None => Err(ErrorCode::UnknownQuery(format!(
            "Not found query {}",
            query_id
        ))),
        Some(profiles) => Ok(profiles.unwrap()),
    }
}

pub fn get_profile_from_cache(target: &str) -> Result<Vec<PlanProfile>, ErrorCode> {
    let profiles_queue = ProfilesLogQueue::instance()?;
    for element in profiles_queue.data.read().event_queue.iter().flatten() {
        if element.query_id == target {
            return Ok(element.profiles.clone());
        }
    }
    Err(ErrorCode::UnknownQuery(format!(
        "Not found query {}",
        target
    )))
}
