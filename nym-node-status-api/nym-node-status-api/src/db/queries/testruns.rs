use crate::db::DbConnection;
use crate::db::DbPool;
use crate::db::models::{TestRunDto, TestRunKind, TestRunStatus};
use crate::http::models::TestrunAssignment;
use crate::utils::now_utc;
use nym_gateway_probe::PortCheckResult;
use time::Duration;

pub(crate) async fn count_testruns_in_progress(
    conn: &mut DbConnection,
) -> anyhow::Result<Option<i64>> {
    sqlx::query_scalar!(
        r#"SELECT
            COUNT(id) as "count: i64"
         FROM testruns
         WHERE
            status = $1
         "#,
        TestRunStatus::InProgress as i64,
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(anyhow::Error::from)
}

pub(crate) async fn get_in_progress_testrun_by_id(
    conn: &mut DbConnection,
    testrun_id: i32,
) -> anyhow::Result<TestRunDto> {
    sqlx::query_as!(
        TestRunDto,
        r#"SELECT
            id as "id!",
            gateway_id as "gateway_id!",
            status as "status!",
            kind as "kind!",
            created_utc as "created_utc!",
            ip_address as "ip_address!",
            log as "log!",
            last_assigned_utc
         FROM testruns
         WHERE
            id = $1
         AND
            status = $2
         ORDER BY created_utc
         LIMIT 1"#,
        testrun_id,
        TestRunStatus::InProgress as i64,
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(|e| anyhow::anyhow!("Couldn't retrieve testrun {testrun_id}: {e}"))
}

pub(crate) async fn update_testruns_assigned_before(
    db: &DbPool,
    max_age: Duration,
) -> anyhow::Result<u64> {
    let mut conn = db.acquire().await?;
    let previous_run = now_utc() - max_age;
    let cutoff_timestamp = previous_run.unix_timestamp();

    let res = sqlx::query!(
        r#"UPDATE
            testruns
        SET
            status = $1
        WHERE
            status = $2
        AND
            last_assigned_utc < $3
            "#,
        TestRunStatus::Queued as i64,
        TestRunStatus::InProgress as i64,
        cutoff_timestamp
    )
    .execute(conn.as_mut())
    .await?;

    let stale_testruns = res.rows_affected();
    if stale_testruns > 0 {
        tracing::info!(
            "Refreshed {} stale testruns, assigned before {} but not yet finished",
            stale_testruns,
            previous_run
        );
    }

    Ok(stale_testruns)
}

pub(crate) async fn assign_oldest_testrun(
    conn: &mut DbConnection,
) -> anyhow::Result<Option<TestrunAssignment>> {
    assign_oldest_testrun_by_kind(conn, TestRunKind::Probe).await
}

pub(crate) async fn assign_oldest_ports_check_testrun(
    conn: &mut DbConnection,
) -> anyhow::Result<Option<TestrunAssignment>> {
    assign_oldest_testrun_by_kind(conn, TestRunKind::PortsCheck).await
}

async fn assign_oldest_testrun_by_kind(
    conn: &mut DbConnection,
    kind: TestRunKind,
) -> anyhow::Result<Option<TestrunAssignment>> {
    let now = now_utc().unix_timestamp();
    // find & mark as "In progress" in the same transaction to avoid race conditions
    // lock the row to avoid two threads reading the same value
    let returning = sqlx::query!(
        r#"
        WITH oldest_queued AS (
            SELECT id
            FROM testruns
            WHERE status = $1 AND kind = $4
            ORDER BY created_utc asc
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE testruns
            SET
                status = $3,
                last_assigned_utc = $2
            FROM oldest_queued
            WHERE testruns.id = oldest_queued.id
        RETURNING
            testruns.id,
            testruns.gateway_id
            "#,
        TestRunStatus::Queued as i32,
        now,
        TestRunStatus::InProgress as i32,
        kind as i16,
    )
    .fetch_optional(conn.as_mut())
    .await?;

    if let Some(testrun) = returning {
        let gw_identity = sqlx::query!(
            r#"
                SELECT
                    id,
                    gateway_identity_key
                FROM gateways
                WHERE id = $1
                LIMIT 1"#,
            testrun.gateway_id
        )
        .fetch_one(conn.as_mut())
        .await?;

        Ok(Some(TestrunAssignment {
            testrun_id: testrun.id,
            gateway_identity_key: gw_identity.gateway_identity_key,
            assigned_at_utc: now,
        }))
    } else {
        Ok(None)
    }
}

pub(crate) async fn update_testrun_status(
    conn: &mut DbConnection,
    testrun_id: i32,
    status: TestRunStatus,
) -> anyhow::Result<()> {
    let status = status as i32;
    sqlx::query!(
        "UPDATE testruns SET status = $1 WHERE id = $2",
        status,
        testrun_id,
    )
    .execute(conn.as_mut())
    .await?;

    Ok(())
}

pub(crate) async fn update_gateway_last_probe_log(
    conn: &mut DbConnection,
    gateway_pk: i32,
    log: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        "UPDATE gateways SET last_probe_log = $1 WHERE id = $2",
        log,
        gateway_pk,
    )
    .execute(conn.as_mut())
    .await
    .map(drop)
    .map_err(From::from)
}

pub(crate) async fn update_gateway_last_probe_result(
    conn: &mut DbConnection,
    gateway_pk: i32,
    result: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        "UPDATE gateways SET last_probe_result = $1 WHERE id = $2",
        result,
        gateway_pk,
    )
    .execute(conn.as_mut())
    .await
    .map(drop)
    .map_err(From::from)
}

pub(crate) async fn update_gateway_last_ports_check_utc(
    conn: &mut DbConnection,
    gateway_pk: i32,
    now_utc: i64,
) -> anyhow::Result<()> {
    sqlx::query!(
        "UPDATE gateways SET last_ports_check_utc = $1, last_updated_utc = $1 WHERE id = $2",
        now_utc,
        gateway_pk,
    )
    .execute(conn.as_mut())
    .await
    .map(drop)
    .map_err(From::from)
}

pub(crate) async fn get_gateway_last_probe_result(
    conn: &mut DbConnection,
    gateway_pk: i32,
) -> anyhow::Result<Option<String>> {
    sqlx::query_scalar!(
        r#"SELECT last_probe_result FROM gateways WHERE id = $1"#,
        gateway_pk
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(From::from)
}

pub(crate) async fn persist_ports_check_result(
    conn: &mut DbConnection,
    gateway_pk: i32,
    port_check_result: &PortCheckResult,
) -> anyhow::Result<()> {
    let now = now_utc().unix_timestamp();

    let failed_ports: Vec<u16> = port_check_result
        .ports
        .iter()
        .filter_map(|(port, open)| {
            if *open {
                None
            } else {
                port.parse::<u16>().ok()
            }
        })
        .collect();

    let all_pass = port_check_result.can_register
        && port_check_result.error.is_none()
        && !port_check_result.ports.is_empty()
        && failed_ports.is_empty();

    let ports_check_value = serde_json::json!({
        "all_pass": all_pass,
        "failed_ports": failed_ports,
        "error": port_check_result.error,
        "ports_tested": port_check_result.ports.len(),
    });

    let mut existing: serde_json::Value =
        match get_gateway_last_probe_result(conn, gateway_pk).await? {
            Some(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
            None => serde_json::Value::Null,
        };

    if !existing.is_object() {
        existing = serde_json::json!({});
    }
    if let Some(obj) = existing.as_object_mut() {
        obj.insert("ports_check".to_string(), ports_check_value);
    }

    let merged = serde_json::to_string(&existing)?;
    update_gateway_last_probe_result(conn, gateway_pk, &merged).await?;
    update_gateway_last_ports_check_utc(conn, gateway_pk, now).await?;
    Ok(())
}

pub(crate) async fn enqueue_due_ports_check_testruns(db: &DbPool) -> anyhow::Result<u64> {
    let mut conn = db.acquire().await?;
    let now = now_utc().unix_timestamp();
    // 3 days soft TTL
    let cutoff = now - time::Duration::days(3).whole_seconds();

    let res = sqlx::query!(
        r#"
        INSERT INTO testruns (gateway_id, status, kind, created_utc, last_assigned_utc, ip_address, log)
        SELECT
            gw.id,
            $1,
            $2,
            $3,
            NULL,
            'ports_check_scheduler',
            ''
        FROM gateways gw
        WHERE gw.bonded = true
          AND (gw.last_ports_check_utc IS NULL OR gw.last_ports_check_utc < $4)
          AND NOT EXISTS (
              SELECT 1
              FROM testruns t
              WHERE t.gateway_id = gw.id
                AND t.kind = $2
                AND t.status IN ($1, $5)
          )
        "#,
        TestRunStatus::Queued as i32,
        TestRunKind::PortsCheck as i16,
        now,
        cutoff,
        TestRunStatus::InProgress as i32,
    )
    .execute(conn.as_mut())
    .await?;

    Ok(res.rows_affected())
}

pub(crate) async fn update_gateway_score(
    conn: &mut DbConnection,
    gateway_pk: i32,
) -> anyhow::Result<()> {
    let now = now_utc().unix_timestamp();
    sqlx::query!(
        "UPDATE gateways SET last_testrun_utc = $1, last_updated_utc = $2 WHERE id = $3",
        now,
        now,
        gateway_pk,
    )
    .execute(conn.as_mut())
    .await
    .map(drop)
    .map_err(From::from)
}

pub(crate) async fn get_testrun_by_id(
    conn: &mut DbConnection,
    testrun_id: i32,
) -> anyhow::Result<TestRunDto> {
    sqlx::query_as!(
        TestRunDto,
        r#"SELECT
            id,
            gateway_id,
            status,
            kind,
            created_utc,
            ip_address,
            log,
            last_assigned_utc
         FROM testruns
         WHERE id = $1"#,
        testrun_id
    )
    .fetch_one(conn.as_mut())
    .await
    .map_err(|e| anyhow::anyhow!("Testrun {} not found: {}", testrun_id, e))
}

pub(crate) async fn insert_external_testrun(
    conn: &mut DbConnection,
    testrun_id: i32,
    gateway_id: i32,
    assigned_at_utc: i64,
) -> anyhow::Result<()> {
    let now = crate::utils::now_utc().unix_timestamp();

    sqlx::query!(
        r#"INSERT INTO testruns (
            id,
            gateway_id,
            status,
            kind,
            created_utc,
            last_assigned_utc,
            ip_address,
            log
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        testrun_id,
        gateway_id,
        TestRunStatus::InProgress as i32,
        TestRunKind::Probe as i16,
        now,
        assigned_at_utc,
        "external", // Marker for external origin
        ""
    ) // Empty initial log
    .execute(conn.as_mut())
    .await?;

    tracing::debug!(
        "Created external testrun {} for gateway {}",
        testrun_id,
        gateway_id
    );
    Ok(())
}

pub(crate) async fn update_testrun_status_by_gateway(
    conn: &mut DbConnection,
    gateway_id: i32,
    status: TestRunStatus,
) -> anyhow::Result<()> {
    let status = status as i32;
    sqlx::query!(
        "UPDATE testruns SET status = $1 WHERE gateway_id = $2 AND status = $3",
        status,
        gateway_id,
        TestRunStatus::InProgress as i32
    )
    .execute(conn.as_mut())
    .await?;

    Ok(())
}
