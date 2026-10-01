/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use super::common::{jid, target_query_get};
use super::{Maps, Net, Plan, Uploader};
use crate::error::Error;
use crate::jmap::error::JmapError;
use crate::jmap::request::{
    MethodCall, Request, SetRequest, check_method_error, get_objects, retry_method_call, set_call,
};
use crate::jmap::retry::MethodCallKind;
use crate::jmap::wire::JmapId;
use crate::logging::Logger;
use crate::sync::import_jmap::mapping::{EMAIL_SELECT, EmailRow, TargetResolver, row_to_email};
use crate::sync::keys::{EmailIndex, EmailKey, email_index, email_keys, index_from_json};
use crate::sync::{Context, TypeCounts};
use crate::types::ObjectType;

fn server_index(v: &Value) -> EmailIndex {
    let arr = |k: &str| {
        v.get(k)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.get("email").and_then(Value::as_str).map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let mids: Vec<String> = v
        .get("messageId")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    email_index(
        &mids,
        &arr("from"),
        v.get("subject").and_then(Value::as_str).unwrap_or(""),
        v.get("sentAt").and_then(Value::as_str).unwrap_or(""),
        &arr("to"),
    )
}

pub fn reconcile(
    ctx: &Context,
    net: &Net,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
) -> Result<Plan, Error> {
    let ty = ObjectType::Email;

    let target_min = target_query_get(net, ty, Some(&["messageId"])).map_err(Error::from)?;
    let mut indices: Vec<EmailIndex> = target_min.iter().map(server_index).collect();

    let fallback_ids: Vec<JmapId> = target_min
        .iter()
        .zip(indices.iter())
        .filter(|(_, i)| i.mids.is_empty())
        .filter_map(|(v, _)| jid(v).map(JmapId))
        .collect();
    if !fallback_ids.is_empty() {
        let got = get_objects::<Value>(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            &fallback_ids,
            Some(&["messageId", "from", "subject", "sentAt", "to"]),
            &net.limits,
        )
        .map_err(Error::from)?;
        let by_id: HashMap<String, &Value> = got
            .list
            .iter()
            .filter_map(|v| jid(v).map(|i| (i, v)))
            .collect();
        for (v, slot) in target_min.iter().zip(indices.iter_mut()) {
            if let Some(full) = jid(v).and_then(|i| by_id.get(&i)) {
                *slot = server_index(full);
            }
        }
    }
    let target_keys: HashSet<EmailKey> = email_keys(&indices).into_iter().collect();

    let local: Vec<(i64, EmailRow)> = {
        let mut stmt = ctx
            .conn
            .prepare(EMAIL_SELECT)
            .map_err(|e| Error::Partial(e.to_string()))?;
        stmt.query_map([], |row| {
            let id: i64 = row.get(0)?;
            Ok((id, row_to_email(row)))
        })
        .and_then(|m| m.collect::<Result<Vec<_>, _>>())
        .map_err(|e| Error::Partial(e.to_string()))?
        .into_iter()
        .map(|(id, r)| Ok((id, r.map_err(Error::from)?)))
        .collect::<Result<_, Error>>()?
    };

    let local_indices: Vec<EmailIndex> = local
        .iter()
        .map(|(_, r)| index_from_json(&r.message_match))
        .collect();
    let local_keys = email_keys(&local_indices);

    if net.update_matched {
        let target_ids: Vec<Option<String>> = target_min.iter().map(jid).collect();
        return reconcile_paired(
            ctx,
            net,
            maps,
            counts,
            logger,
            &target_ids,
            &email_keys(&indices),
            &local,
            &local_keys,
        );
    }

    let mut uploader = Uploader::new(net, &ctx.conn);
    for (i, key) in local_keys.iter().enumerate() {
        if target_keys.contains(key) {
            counts.skipped += 1;
            continue;
        }
        let (local_id, row) = &local[i];
        export_one(net, &mut uploader, maps, *local_id, row, counts, logger);
    }

    Ok(Plan::default())
}

/// (archive row index, its mailboxIds on the target, paired target email id)
type Paired = (usize, Option<Map<String, Value>>, String);

/// `--update-matched`: pairs archive emails with target emails one-to-one per
/// identity key (a key can repeat, e.g. one message filed in two folders as two
/// emails). Pairs whose mailboxes or keywords differ are updated to the archive's
/// state, archive emails left unpaired are created, and target emails left
/// unpaired become prune candidates (destroyed only with `--prune`). Pairing
/// prefers a target email already in the same mailboxes.
#[allow(clippy::too_many_arguments)]
fn reconcile_paired(
    ctx: &Context,
    net: &Net,
    maps: &mut Maps,
    counts: &mut TypeCounts,
    logger: &Logger,
    target_ids: &[Option<String>],
    target_keys: &[EmailKey],
    local: &[(i64, EmailRow)],
    local_keys: &[EmailKey],
) -> Result<Plan, Error> {
    let ty = ObjectType::Email;

    // Current mailboxes and keywords of every target email.
    let ids: Vec<JmapId> = target_ids.iter().flatten().cloned().map(JmapId).collect();
    let mut state: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
    if !ids.is_empty() {
        let got = get_objects::<Value>(
            &net.client,
            &net.api,
            &net.account,
            ty.jmap_name(),
            &ids,
            Some(&["mailboxIds", "keywords"]),
            &net.limits,
        )
        .map_err(Error::from)?;
        for v in &got.list {
            if let Some(id) = jid(v) {
                state.insert(
                    id,
                    (true_keys(v.get("mailboxIds"), false), true_keys(v.get("keywords"), true)),
                );
            }
        }
    }

    let mut by_key: HashMap<&EmailKey, Vec<String>> = HashMap::new();
    for (key, id) in target_keys.iter().zip(target_ids) {
        if let Some(id) = id {
            by_key.entry(key).or_default().push(id.clone());
        }
    }

    let mut groups: Vec<(&EmailKey, Vec<usize>)> = Vec::new();
    let mut group_of: HashMap<&EmailKey, usize> = HashMap::new();
    for (i, key) in local_keys.iter().enumerate() {
        let g = *group_of.entry(key).or_insert_with(|| {
            groups.push((key, Vec::new()));
            groups.len() - 1
        });
        groups[g].1.push(i);
    }

    let mut updates = Map::new();
    let mut unpaired_local: Vec<usize> = Vec::new();
    for (key, locals) in &groups {
        let mut free: Vec<String> = by_key.remove(*key).unwrap_or_default();
        // First pass: a target email in the same mailboxes; second pass: whatever is left.
        let mut pairs: Vec<Paired> = Vec::new();
        let mut rest: Vec<(usize, Option<Map<String, Value>>)> = Vec::new();
        for &i in locals {
            let mids = build_mailbox_ids(&local[i].1, maps);
            let want = mids.as_ref().map(|m| sorted(m.keys().cloned().collect()));
            let pos = want.as_ref().and_then(|w| {
                free.iter()
                    .position(|t| state.get(t).is_some_and(|(mb, _)| mb == w))
            });
            match pos {
                Some(p) => pairs.push((i, mids, free.remove(p))),
                None => rest.push((i, mids)),
            }
        }
        for (i, mids) in rest {
            if free.is_empty() {
                unpaired_local.push(i);
            } else {
                pairs.push((i, mids, free.remove(0)));
            }
        }
        // Targets left for this key are copies the archive no longer has.
        by_key.insert(*key, free);

        for (i, mids, tid) in pairs {
            let Some(mids) = mids else {
                logger.warn(&format!(
                    "Email/set update {tid} skipped: mailbox not on target"
                ));
                counts.failed += 1;
                continue;
            };
            let kw = build_keywords(&local[i].1);
            let want_mb = sorted(mids.keys().cloned().collect());
            let want_kw = sorted(kw.keys().map(|k| k.to_ascii_lowercase()).collect());
            let same = state
                .get(&tid)
                .is_some_and(|(mb, k)| *mb == want_mb && *k == want_kw);
            if same {
                counts.skipped += 1;
            } else {
                updates.insert(
                    tid,
                    json!({ "mailboxIds": Value::Object(mids), "keywords": Value::Object(kw) }),
                );
            }
        }
    }

    if !updates.is_empty() {
        let n = updates.len() as u64;
        if net.dry_run {
            counts.updated += n;
        } else {
            match set_call(
                &net.client,
                &net.api,
                &net.account,
                ty.jmap_name(),
                SetRequest {
                    update: Some(Value::Object(updates)),
                    ..Default::default()
                },
                &net.limits,
            ) {
                Ok(outcome) => {
                    counts.updated += outcome.updated.len() as u64;
                    for (id, err) in &outcome.not_updated {
                        logger.warn(&format!("Email/set update {id} failed: {err}"));
                        counts.failed += 1;
                    }
                }
                Err(e) => {
                    logger.warn(&format!("Email/set update request failed: {e}"));
                    counts.failed += n;
                }
            }
        }
    }

    let mut uploader = Uploader::new(net, &ctx.conn);
    for i in unpaired_local {
        let (local_id, row) = &local[i];
        export_one(net, &mut uploader, maps, *local_id, row, counts, logger);
    }

    let mut plan = Plan::default();
    if net.prune {
        plan.prune_candidates = by_key.into_values().flatten().collect();
        plan.prune_candidates.sort();
    }
    Ok(plan)
}

/// Keys of a JMAP `{key: true}` map, sorted (lowercased for keywords).
fn true_keys(v: Option<&Value>, lower: bool) -> Vec<String> {
    let keys = v
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter(|(_, b)| b.as_bool().unwrap_or(false))
                .map(|(k, _)| if lower { k.to_ascii_lowercase() } else { k.clone() })
                .collect()
        })
        .unwrap_or_default();
    sorted(keys)
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}

fn build_mailbox_ids(row: &EmailRow, maps: &Maps) -> Option<Map<String, Value>> {
    let mut mids = Map::new();
    for ml in &row.mailbox_locals {
        let t = maps.target(ObjectType::Mailbox, *ml)?;
        mids.insert(t.0, Value::Bool(true));
    }
    Some(mids)
}

fn build_keywords(row: &EmailRow) -> Map<String, Value> {
    let mut kw = Map::new();
    for k in &row.keywords {
        kw.insert(k.clone(), Value::Bool(true));
    }
    kw
}

fn blob_hint(uploader: &Uploader, row: &EmailRow) -> String {
    let idx = index_from_json(&row.message_match);
    let mut s = match idx.mids.first() {
        Some(mid) => format!("message-id <{mid}>"),
        None => "no message-id".to_owned(),
    };
    if let Some(len) = uploader.blob_len(row.blob_local_id) {
        use std::fmt::Write;
        let _ = write!(s, ", {}", crate::inspect::format_bytes(len));
    }
    s
}

fn size_note(e: &JmapError) -> &'static str {
    if matches!(
        e,
        JmapError::RequestTooLarge | JmapError::SingleObjectTooLarge(_)
    ) {
        "; exceeds the target server size limit, so this message is skipped and re-running will not migrate it"
    } else {
        ""
    }
}

fn import_item(
    blob: String,
    mids: Map<String, Value>,
    kw: Map<String, Value>,
    received_at: &str,
) -> Value {
    json!({
        "blobId": blob,
        "mailboxIds": Value::Object(mids),
        "keywords": Value::Object(kw),
        "receivedAt": received_at,
    })
}

fn export_one(
    net: &Net,
    uploader: &mut Uploader,
    maps: &Maps,
    local_id: i64,
    row: &EmailRow,
    counts: &mut TypeCounts,
    logger: &Logger,
) {
    let cid = format!("e{local_id}");
    let mids = match build_mailbox_ids(row, maps) {
        Some(m) => m,
        None => {
            logger.warn(&format!(
                "Email/import {cid} ({}) skipped: mailbox not on target",
                blob_hint(uploader, row)
            ));
            counts.failed += 1;
            return;
        }
    };
    let blob = match uploader.upload_with(row.blob_local_id, "message/rfc822") {
        Ok(b) => b.0,
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob upload failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            counts.failed += 1;
            return;
        }
    };
    if net.dry_run {
        counts.created += 1;
        return;
    }
    let item = import_item(blob, mids, build_keywords(row), &row.received_at);
    match send_single_import(net, &cid, item, logger) {
        Ok(SingleImport::Created) => counts.created += 1,
        Ok(SingleImport::Skipped) => counts.skipped += 1,
        Ok(SingleImport::NotCreated { error_type, .. }) if error_type == "blobNotFound" => {
            retry_after_reupload(net, uploader, maps, &cid, row, counts, logger);
        }
        Ok(SingleImport::NotCreated { detail, .. }) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) failed: {detail}",
                blob_hint(uploader, row)
            ));
            counts.failed += 1;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) send failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            counts.failed += 1;
        }
    }
}

fn retry_after_reupload(
    net: &Net,
    uploader: &mut Uploader,
    maps: &Maps,
    cid: &str,
    row: &EmailRow,
    counts: &mut TypeCounts,
    logger: &Logger,
) {
    uploader.invalidate(row.blob_local_id);
    let blob = match uploader.upload_with(row.blob_local_id, "message/rfc822") {
        Ok(b) => b.0,
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) blob re-upload failed: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            counts.failed += 1;
            return;
        }
    };
    let mids = match build_mailbox_ids(row, maps) {
        Some(m) => m,
        None => {
            logger.warn(&format!(
                "Email/import {cid} ({}) skipped: mailbox not on target",
                blob_hint(uploader, row)
            ));
            counts.failed += 1;
            return;
        }
    };
    let item = import_item(blob, mids, build_keywords(row), &row.received_at);
    match send_single_import(net, cid, item, logger) {
        Ok(SingleImport::Created) => counts.created += 1,
        Ok(SingleImport::Skipped) => counts.skipped += 1,
        Ok(SingleImport::NotCreated { detail, .. }) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) failed after blob re-upload: {detail}",
                blob_hint(uploader, row)
            ));
            counts.failed += 1;
        }
        Err(e) => {
            logger.warn(&format!(
                "Email/import {cid} ({}) send failed after blob re-upload: {e}{}",
                blob_hint(uploader, row),
                size_note(&e)
            ));
            counts.failed += 1;
        }
    }
}

enum SingleImport {
    Created,
    Skipped,
    NotCreated { error_type: String, detail: String },
}

fn send_single_import(
    net: &Net,
    cid: &str,
    item: Value,
    logger: &Logger,
) -> Result<SingleImport, JmapError> {
    let mut emails = Map::new();
    emails.insert(cid.to_owned(), item);
    let mut req = Request::new();
    req.call(
        "Email/import",
        json!({ "accountId": net.account, "emails": Value::Object(emails) }),
        "i",
    );
    req.fits(&net.limits)?;
    retry_method_call(
        &net.client,
        MethodCallKind::SingleObjectWrite,
        logger,
        || interpret_import(req.send(&net.client, &net.api)?.first()?, cid),
    )
}

fn interpret_import(mr: &MethodCall, cid: &str) -> Result<SingleImport, JmapError> {
    check_method_error(mr)?;
    if let Some(err) = mr
        .args
        .get("notCreated")
        .and_then(Value::as_object)
        .and_then(|nc| nc.get(cid))
    {
        let error_type = err
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if error_type == "alreadyExists" {
            return Ok(SingleImport::Skipped);
        }
        return Ok(SingleImport::NotCreated {
            error_type,
            detail: err.to_string(),
        });
    }
    if mr
        .args
        .get("created")
        .and_then(Value::as_object)
        .is_some_and(|c| !c.is_empty())
    {
        return Ok(SingleImport::Created);
    }
    Ok(SingleImport::NotCreated {
        error_type: String::new(),
        detail: format!("Email/import returned neither created nor notCreated for {cid}"),
    })
}
