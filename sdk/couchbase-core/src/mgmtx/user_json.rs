/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::mgmtx::error;
use crate::mgmtx::user::{
    Group, Origin, Role, RoleAndDescription, RoleAndOrigins, User, UserAndMetadata,
};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct RoleJson {
    pub role: String,
    #[serde(rename = "bucket_name", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "scope_name", skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(rename = "collection_name", skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct RoleAndDescriptionJson {
    #[serde(flatten)]
    pub role_json: RoleJson,
    #[serde(rename = "name")]
    pub display_name: String,
    #[serde(rename = "desc")]
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct OriginJson {
    #[serde(rename = "type")]
    pub origin_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct RoleAndOriginsJson {
    #[serde(flatten)]
    pub role_json: RoleJson,
    pub origins: Vec<OriginJson>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct GroupJson {
    pub id: String,
    pub description: Option<String>,
    pub roles: Vec<RoleJson>,
    #[serde(rename = "ldap_group_ref", skip_serializing_if = "Option::is_none")]
    pub ldap_group_reference: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialOrd, PartialEq)]
pub struct UserJson {
    pub id: String,
    pub name: String,
    pub groups: Vec<String>,
    pub roles: Vec<RoleJson>,

    pub password: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct UserAndMetadataJson {
    pub id: String,
    pub name: String,
    pub roles: Vec<RoleAndOriginsJson>,
    pub groups: Vec<String>,
    pub domain: String,
    pub password_changed_date: Option<String>,
    pub external_groups: Vec<String>,
}

impl From<RoleJson> for Role {
    fn from(role_json: RoleJson) -> Self {
        Self {
            name: role_json.role,
            bucket: role_json.bucket,
            scope: role_json.scope,
            collection: role_json.collection,
        }
    }
}

impl From<OriginJson> for Origin {
    fn from(origin_json: OriginJson) -> Self {
        Self {
            origin_type: origin_json.origin_type,
            name: origin_json.name,
        }
    }
}

impl From<RoleAndOriginsJson> for RoleAndOrigins {
    fn from(role_and_origins_json: RoleAndOriginsJson) -> Self {
        Self {
            role: role_and_origins_json.role_json.into(),
            origins: role_and_origins_json
                .origins
                .into_iter()
                .map(Origin::from)
                .collect(),
        }
    }
}

impl From<RoleAndDescriptionJson> for RoleAndDescription {
    fn from(role_and_description_json: RoleAndDescriptionJson) -> Self {
        Self {
            role: role_and_description_json.role_json.into(),
            display_name: role_and_description_json.display_name,
            description: role_and_description_json.description,
        }
    }
}

impl From<GroupJson> for Group {
    fn from(group_json: GroupJson) -> Self {
        Self {
            name: group_json.id,
            description: group_json.description,
            roles: group_json.roles.into_iter().map(Role::from).collect(),
            ldap_group_reference: group_json.ldap_group_reference,
        }
    }
}

impl TryFrom<UserAndMetadataJson> for UserAndMetadata {
    type Error = error::Error;

    fn try_from(val: UserAndMetadataJson) -> Result<UserAndMetadata, Self::Error> {
        let password_changed = if let Some(pc) = val.password_changed_date {
            Some(DateTime::parse_from_rfc3339(&pc).map_err(|e| {
                error::Error::new_message_error(format!("failed to parse date: {}", e))
            })?)
        } else {
            None
        };

        let mut roles = vec![];
        let mut effective_roles = vec![];
        for role_data in val.roles {
            let effective_role: RoleAndOrigins = role_data.into();
            effective_roles.push(effective_role.clone());

            let role = effective_role.role;
            if effective_role.origins.is_empty() {
                roles.push(role.clone());
            } else {
                for origin in effective_role.origins {
                    if origin.origin_type == "user" {
                        roles.push(role.clone());
                    }
                }
            }
        }

        Ok(UserAndMetadata {
            domain: val.domain,
            user: User {
                username: val.id,
                display_name: val.name,
                groups: val.groups,
                roles,
                password: None,
            },
            effective_roles,
            password_changed,
            external_groups: val.external_groups,
        })
    }
}
